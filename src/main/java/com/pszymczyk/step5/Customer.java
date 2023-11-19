package com.pszymczyk.step5;

record Customer(String name, String age) {

    @Override
    public String toString() {
        return "Hooray, it's correctly deserialized Customer{" +
            "name='" + name + '\'' +
            ", age='" + age + '\'' +
            '}';
    }
}
