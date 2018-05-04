package reduce;

public class MyPair<T,K> {
    private T leftVal;
    private K rightVal;

    public MyPair(T leftVal, K rightVal){
        this.leftVal = leftVal;
        this.rightVal = rightVal;
    }

    public T getLeftVal() {
        return leftVal;
    }

    public void setLeftVal(T leftVal) {
        this.leftVal = leftVal;
    }

    public K getRightVal() {
        return rightVal;
    }

    public void setRightVal(K rightVal) {
        this.rightVal = rightVal;
    }
}
