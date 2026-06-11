package ru.nsu.martynov.model;

import jakarta.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
public class PersonReference {
    @XmlAttribute(name = "ref", required = true)
    private String ref;

    public PersonReference() {}

    public PersonReference(String ref) {
        this.ref = ref;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }
}