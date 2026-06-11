package ru.nsu.martynov.parser;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PersonMerger {
    private final Map<String, RawPerson> byId = new HashMap<>();
    private final Map<String, RawPerson> byName = new HashMap<>();

    public void merge(RawPerson fragment) {
        normalize(fragment);

        String nameKey = fragment.getNameKey();
        RawPerson targetById = fragment.id != null ? byId.get(fragment.id) : null;
        RawPerson targetByName = nameKey != null ? byName.get(nameKey) : null;

        RawPerson target;

        if (targetById != null && targetByName != null) {
            if (targetById == targetByName) {
                target = targetById;
            } else {
                // старое решение: объединяем в объект по id
                target = targetById;
                target.merge(targetByName);
            }
            target.merge(fragment);

        } else if (targetById != null) {
            target = targetById;
            target.merge(fragment);

        } else if (targetByName != null) {
            target = targetByName;
            target.merge(fragment);

        } else {
            target = new RawPerson();
            target.merge(fragment);
        }

        // Обновляем индексы только на canonical-объект
        if (target.id != null) {
            byId.put(target.id, target);
        }
        String targetName = target.getNameKey();
        if (targetName != null) {
            byName.put(targetName, target);
        }
    }

    public Map<String, RawPerson> getById() {
        return byId;
    }

    public Map<String, RawPerson> getByName() {
        return byName;
    }

    private void normalize(RawPerson p) {
        if (p == null) return;

        p.id = trimToNull(p.id);
        p.firstname = trimToNull(p.firstname);
        p.surname = trimToNull(p.surname);
        p.fullNameFromAttr = trimToNull(p.fullNameFromAttr);
        p.gender = normalizeGender(p.gender);

        p.spouseRefs = normalizeSet(p.spouseRefs);
        p.parentRefs = normalizeSet(p.parentRefs);
        p.childRefs = normalizeSet(p.childRefs);
        p.siblingRefs = normalizeSet(p.siblingRefs);
    }

    private Set<String> normalizeSet(Set<String> src) {
        Set<String> result = new HashSet<>();
        if (src == null) return result;

        for (String s : src) {
            String t = trimToNull(s);
            if (t != null) result.add(t);
        }
        return result;
    }

    private String normalizeGender(String g) {
        g = trimToNull(g);
        if (g == null) return null;

        String x = g.toLowerCase();
        if (x.equals("m") || x.equals("male")) return "male";
        if (x.equals("f") || x.equals("female")) return "female";
        return g;
    }

    private String trimToNull(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}