package ru.nsu.martynov.parser;

import ru.nsu.martynov.model.People;
import ru.nsu.martynov.model.Person;
import ru.nsu.martynov.model.PersonReference;
import ru.nsu.martynov.model.Relations;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.util.*;

/**
 * Reads the source XML, merges duplicate person fragments into canonical RawPerson records,
 * then builds the final JAXB model.
 */
public class DataProcessor {

    private final PersonMerger merger = new PersonMerger();

    public People processData(InputStream xmlStream) throws Exception {
        parseStAX(xmlStream);
        return buildAndValidateFinalData();
    }

    private void parseStAX(InputStream xmlStream) throws Exception {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        XMLStreamReader reader = factory.createXMLStreamReader(xmlStream);

        RawPerson current = null;
        String currentTag = null;
        StringBuilder textBuffer = new StringBuilder();

        while (reader.hasNext()) {
            int event = reader.next();

            switch (event) {
                case XMLStreamConstants.START_ELEMENT -> {
                    String tag = reader.getLocalName();
                    currentTag = tag;
                    textBuffer.setLength(0);

                    if ("person".equals(tag)) {
                        current = new RawPerson();
                        current.id = normalize(reader.getAttributeValue(null, "id"));
                        current.fullNameFromAttr = normalize(reader.getAttributeValue(null, "name"));
                    }

                    if (current != null) {
                        extractAttributes(tag, reader, current);
                    }
                }

                case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA -> {
                    if (current != null && currentTag != null) {
                        String chunk = reader.getText();
                        if (chunk != null) textBuffer.append(chunk);
                    }
                }

                case XMLStreamConstants.END_ELEMENT -> {
                    String tag = reader.getLocalName();

                    if (current != null && currentTag != null && tag.equals(currentTag)) {
                        String text = normalize(textBuffer.toString());
                        if (text != null) {
                            extractText(tag, text, current);
                        }
                        textBuffer.setLength(0);
                    }

                    if ("person".equals(tag) && current != null) {
                        merger.merge(current);
                        current = null;
                        currentTag = null;
                        textBuffer.setLength(0);
                    }
                }
            }
        }

        reader.close();
    }

    private void extractAttributes(String tag, XMLStreamReader reader, RawPerson current) {
        String value = normalize(reader.getAttributeValue(null, "value"));
        String id = normalize(reader.getAttributeValue(null, "id"));
        String val = normalize(reader.getAttributeValue(null, "val"));

        switch (tag) {
            case "firstname" -> {
                if (value != null) current.firstname = value;
            }
            case "surname" -> {
                if (value != null) current.surname = value;
            }
            case "gender" -> {
                if (value != null) current.gender = value;
            }
            case "id" -> {
                if (value != null) current.id = value;
            }
            case "husband", "wife", "spouce" -> {
                if (value != null && !"NONE".equalsIgnoreCase(value)) current.spouseRefs.add(value);
            }
            case "parent" -> {
                if (value != null && !"UNKNOWN".equalsIgnoreCase(value)) current.parentRefs.add(value);
            }
            case "son", "daughter" -> {
                if (id != null) current.childRefs.add(id);
            }
            case "siblings" -> {
                if (val != null) {
                    for (String ref : val.split("\\s+")) {
                        String cleaned = normalize(ref);
                        if (cleaned != null) current.siblingRefs.add(cleaned);
                    }
                }
            }
            case "children-number" -> {
                if (value != null) current.childrenNumber = parseIntSafe(value);
            }
            case "siblings-number" -> {
                if (value != null) current.siblingsNumber = parseIntSafe(value);
            }
        }
    }

    private void extractText(String tag, String text, RawPerson current) {
        switch (tag) {
            case "firstname", "first" -> {
                if (current.firstname == null) current.firstname = text;
            }
            case "surname", "family-name", "family" -> {
                if (current.surname == null) current.surname = text;
            }
            case "gender" -> {
                if (current.gender == null) current.gender = text;
            }
            case "father", "mother" -> current.parentRefs.add(text);
            case "husband", "wife", "spouce" -> current.spouseRefs.add(text);
            case "child" -> current.childRefs.add(text);
            case "brother", "sister" -> current.siblingRefs.add(text);
        }
    }

    private People buildAndValidateFinalData() {
        Collection<RawPerson> uniqueRawPersons = merger.getById().values();

        Map<String, Person> finalPersons = new LinkedHashMap<>();

        for (RawPerson raw : uniqueRawPersons) {
            if (raw.id == null) continue;

            Person p = new Person();
            p.setId(raw.id);
            p.setFirstname(raw.firstname != null ? raw.firstname : "");
            p.setSurname(raw.surname != null ? raw.surname : "");
            p.setGender(raw.gender != null ? raw.normalizedGender() : "");

            finalPersons.put(raw.id, p);
        }

        for (RawPerson raw : uniqueRawPersons) {
            if (raw.id == null) continue;

            Person currentPerson = finalPersons.get(raw.id);
            if (currentPerson == null) continue;

            Relations rel = currentPerson.getRelations();

            Set<String> actualParentIds = resolveToIds(raw.parentRefs);
            Set<String> actualSiblingIds = resolveToIds(raw.siblingRefs);
            Set<String> actualSpouseIds = resolveToIds(raw.spouseRefs);
            Set<String> actualChildIds = resolveToIds(raw.childRefs);

            for (String parentId : actualParentIds) {
                Person parent = finalPersons.get(parentId);
                if (parent == null) continue;

                String g = parent.getGender();
                if ("male".equalsIgnoreCase(g)) {
                    rel.setFather(new PersonReference(parentId));
                } else if ("female".equalsIgnoreCase(g)) {
                    rel.setMother(new PersonReference(parentId));
                } else if (rel.getParent() == null) {
                    rel.setParent(new PersonReference(parentId));
                }
            }

            for (String spouseId : actualSpouseIds) {
                Person spouse = finalPersons.get(spouseId);
                if (spouse == null) continue;

                String g = spouse.getGender();
                if ("male".equalsIgnoreCase(g)) {
                    rel.setHusband(new PersonReference(spouseId));
                } else if ("female".equalsIgnoreCase(g)) {
                    rel.setWife(new PersonReference(spouseId));
                } else {
                    rel.setSpouse(new PersonReference(spouseId));
                }
            }

            for (String childId : actualChildIds) {
                Person child = finalPersons.get(childId);
                if (child == null) continue;

                String g = child.getGender();
                if ("male".equalsIgnoreCase(g)) {
                    rel.getSons().add(new PersonReference(childId));
                } else if ("female".equalsIgnoreCase(g)) {
                    rel.getDaughters().add(new PersonReference(childId));
                } else {
                    rel.getChildren().add(new PersonReference(childId));
                }
            }

            for (String sibId : actualSiblingIds) {
                Person sib = finalPersons.get(sibId);
                if (sib == null) continue;

                String g = sib.getGender();
                if ("male".equalsIgnoreCase(g)) {
                    rel.getBrothers().add(new PersonReference(sibId));
                } else if ("female".equalsIgnoreCase(g)) {
                    rel.getSisters().add(new PersonReference(sibId));
                } else {
                    rel.getSiblings().add(new PersonReference(sibId));
                }
            }
        }

        People people = new People();
        people.getPersons().addAll(finalPersons.values());
        return people;
    }

    private Set<String> resolveToIds(Set<String> refs) {
        Set<String> result = new LinkedHashSet<>();
        for (String ref : refs) {
            String cleaned = normalize(ref);
            if (cleaned == null) continue;

            if (merger.getById().containsKey(cleaned)) {
                result.add(cleaned);
                continue;
            }

            RawPerson byNamePerson = merger.getByName().get(cleaned);
            if (byNamePerson != null && byNamePerson.id != null) {
                result.add(byNamePerson.id);
            }
        }
        return result;
    }

    private String normalize(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private Integer parseIntSafe(String s) {
        try {
            return Integer.parseInt(s.trim());
        } catch (Exception e) {
            return null;
        }
    }
}