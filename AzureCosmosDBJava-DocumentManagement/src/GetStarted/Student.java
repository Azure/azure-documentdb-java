package GetStarted;

public class Student {
    public String getLastName() {
        return LastName;
    }

    public void setLastName(String LastName) {
        this.LastName = LastName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public int getGrade() {
        return grade;
    }

    public void setGrade(int grade) {
        this.grade = grade;
    }

    public Marks[] getMarks() {
        return marks;
    }

    public void setMarks(Marks[] marks) {
        this.marks = marks;
    }

    private String LastName;
    private String firstName;
    private String gender;
    private int grade;
    private Marks[] marks;
}

