package ru.nsu.martynov.model;
import jakarta.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "people")
@XmlAccessorType(XmlAccessType.FIELD)
public class People {
    @XmlElement(name = "person")
    private List<Person> persons = new ArrayList<>();

    public void addPerson(Person person) { this.persons.add(person); }

    public List<Person> getPersons() { return persons; }
}

