package cn.qross.app;

public class OneApiPlain {
    public int id = 0;
    public String defaultValues = "";
    public String statement = "";
    public String title = "";
    public String description = "";
    public String params = "";
    public String returnValue = "";
    public String permit = "";
    public String allowed = "";
    public String creator = "";
    public String mender = "";
    public boolean exampled = false; // if to example or not

    public OneApiPlain() {

    }

    public OneApiPlain(int id, String statement, String defaultValues, boolean exampled) {
        this.id = id;
        this.statement = statement;
        this.defaultValues = defaultValues;
        this.exampled = exampled;
    }
}
