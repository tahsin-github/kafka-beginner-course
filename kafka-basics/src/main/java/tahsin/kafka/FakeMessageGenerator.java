package tahsin.kafka;

import com.github.javafaker.Faker;

public class FakeMessageGenerator {

    public String name;
    public String firstName;
    public String lastName;

    public String Address;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getAddress() {
        return Address;
    }

    public void setAddress(String address) {
        Address = address;
    }

    public FakeMessageGenerator() {
    }

    public FakeMessageGenerator(String name, String firstName, String lastName, String address) {
        this.name = name;
        this.firstName = firstName;
        this.lastName = lastName;
        Address = address;
    }

    @Override
    public String toString() {
        return "FakeMessageGenerator{" +
                "name='" + name + '\'' +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", Address='" + Address + '\'' +
                '}';
    }

    public String getFakeInformation(){
        Faker faker = new Faker();

        String name = faker.name().fullName();
        String firstName = faker.name().firstName();
        String lastName = faker.name().lastName();

        String streetAddress = faker.address().streetAddress();

        FakeMessageGenerator messageGenerator = new FakeMessageGenerator(name, firstName, lastName, streetAddress);

        return messageGenerator.toString();

    }

    public String getRandomIntString(){
        Faker faker = new Faker();

        String key = String.valueOf(faker.number().numberBetween(0, 5));

        return key;

    }



    public static void main(String[] args) {

        FakeMessageGenerator messageGenerator = new FakeMessageGenerator();

        String message = messageGenerator.getFakeInformation();

        System.out.println(message);

//        Faker faker = new Faker();
//
//        String name = faker.name().fullName();
//        String firstName = faker.name().firstName();
//        String lastName = faker.name().lastName();
//
//        String streetAddress = faker.address().streetAddress();
//
//        System.out.println(name + firstName + lastName + streetAddress);


    }
}
