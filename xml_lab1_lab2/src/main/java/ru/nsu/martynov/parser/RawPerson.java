package ru.nsu.martynov.parser;

import java.util.HashSet;
import java.util.Set;

public class RawPerson {
    public String id;
    public String firstname;
    public String surname;
    public String fullNameFromAttr;
    public String gender;

    public Integer childrenNumber;
    public Integer siblingsNumber;

    public Set<String> spouseRefs = new HashSet<>();
    public Set<String> parentRefs = new HashSet<>();
    public Set<String> childRefs = new HashSet<>();
    public Set<String> siblingRefs = new HashSet<>();

    public void merge(RawPerson other) {
        if (other == null) return;

        if (id == null) id = other.id;
        if (firstname == null) firstname = other.firstname;
        if (surname == null) surname = other.surname;
        if (fullNameFromAttr == null) fullNameFromAttr = other.fullNameFromAttr;
        if (gender == null) gender = other.gender;
        if (childrenNumber == null) childrenNumber = other.childrenNumber;
        if (siblingsNumber == null) siblingsNumber = other.siblingsNumber;

        spouseRefs.addAll(other.spouseRefs);
        parentRefs.addAll(other.parentRefs);
        childRefs.addAll(other.childRefs);
        siblingRefs.addAll(other.siblingRefs);
    }

    public String getNameKey() {
        if (firstname == null || surname == null) return null;
        return firstname + " " + surname;
    }

    public String normalizedGender() {
        if (gender == null) return null;
        String g = gender.trim().toLowerCase();
        if (g.equals("m") || g.equals("male")) return "male";
        if (g.equals("f") || g.equals("female")) return "female";
        return gender.trim();
    }

    @Override
    public String toString() {
        return "RawPerson{" +
                "id='" + id + '\'' +
                ", firstname='" + firstname + '\'' +
                ", surname='" + surname + '\'' +
                ", fullNameFromAttr='" + fullNameFromAttr + '\'' +
                ", gender='" + gender + '\'' +
                '}';
    }
}