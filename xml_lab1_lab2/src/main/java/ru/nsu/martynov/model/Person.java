package ru.nsu.martynov.model;

import jakarta.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {"firstname", "surname", "gender", "relations"})
public class Person {
    @XmlAttribute(required = true)
    @XmlID
    private String id;

    @XmlElement
    private String firstname = "";

    @XmlElement
    private String surname = "";

    @XmlElement
    private String gender = "";

    @XmlElement
    private Relations relations = new Relations();

    public void setId(String id) { this.id = id; }
    public String getId() { return id; }

    public void setFirstname(String firstname) { this.firstname = firstname; }
    public String getFirstname() { return firstname; }

    public void setSurname(String surname) { this.surname = surname; }
    public String getSurname() { return surname; }

    public void setGender(String gender) { this.gender = gender; }
    public String getGender() { return gender; }

    public Relations getRelations() { return relations; }
}