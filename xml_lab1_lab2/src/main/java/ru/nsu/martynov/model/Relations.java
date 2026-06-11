package ru.nsu.martynov.model;

import jakarta.xml.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {
        "parent", "father", "mother",
        "spouse", "husband", "wife",
        "child", "sons", "daughters",
        "sibling", "brothers", "sisters"
})
public class Relations {

    @XmlElement(name = "parent")
    private PersonReference parent;

    @XmlElement(name = "father")
    private PersonReference father;

    @XmlElement(name = "mother")
    private PersonReference mother;

    @XmlElement(name = "spouse")
    private PersonReference spouse;

    @XmlElement(name = "husband")
    private PersonReference husband;

    @XmlElement(name = "wife")
    private PersonReference wife;

    @XmlElement(name = "child")
    private List<PersonReference> child = new ArrayList<>();

    @XmlElement(name = "son")
    private List<PersonReference> sons = new ArrayList<>();

    @XmlElement(name = "daughter")
    private List<PersonReference> daughters = new ArrayList<>();

    @XmlElement(name = "sibling")
    private List<PersonReference> sibling = new ArrayList<>();

    @XmlElement(name = "brother")
    private List<PersonReference> brothers = new ArrayList<>();

    @XmlElement(name = "sister")
    private List<PersonReference> sisters = new ArrayList<>();

    public void setParent(PersonReference parent) { this.parent = parent; }
    public void setFather(PersonReference father) { this.father = father; }
    public void setMother(PersonReference mother) { this.mother = mother; }

    public void setSpouse(PersonReference spouse) { this.spouse = spouse; }
    public void setHusband(PersonReference husband) { this.husband = husband; }
    public void setWife(PersonReference wife) { this.wife = wife; }

    public PersonReference getParent() { return parent; }
    public PersonReference getFather() { return father; }
    public PersonReference getMother() { return mother; }

    public PersonReference getSpouse() { return spouse; }
    public PersonReference getHusband() { return husband; }
    public PersonReference getWife() { return wife; }

    public List<PersonReference> getChildren() { return child; }
    public List<PersonReference> getSons() { return sons; }
    public List<PersonReference> getDaughters() { return daughters; }

    public List<PersonReference> getSiblings() { return sibling; }
    public List<PersonReference> getBrothers() { return brothers; }
    public List<PersonReference> getSisters() { return sisters; }

    public boolean hasAnyRelation() {
        return parent != null || father != null || mother != null
                || spouse != null || husband != null || wife != null
                || !child.isEmpty() || !sons.isEmpty() || !daughters.isEmpty()
                || !sibling.isEmpty() || !brothers.isEmpty() || !sisters.isEmpty();
    }
}