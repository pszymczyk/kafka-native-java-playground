package com.pszymczyk.step5;

public class Customer {

    private String name;
    private String age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Hooray, it's correctly deserialized Customer{" +
            "name='" + name + '\'' +
            ", age='" + age + '\'' +
            '}';
    }
}
