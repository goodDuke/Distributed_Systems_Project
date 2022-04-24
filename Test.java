import java.io.Serializable;

public class Test implements Serializable{
    int number;
    boolean flag;

    public Test(int number, boolean flag) {
        this.number = number;
        this.flag = flag;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }
}

