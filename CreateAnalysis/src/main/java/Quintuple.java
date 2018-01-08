/**
 * Created by Sergio Serusi on 25/07/2017.
 */
public class Quintuple<A, B, C, D, E> {
    private A first; //first member of pair
    private B second; //second member of pair
    private C third;
    private D fourth;
    private E fifth;

    public Quintuple(A first, B second, C third, D fourth, E fifth) {
        this.first  = first;
        this.second = second;
        this.third  = third;
        this.fourth = fourth;
        this.fifth  = fifth;
    }

    public A getFirst() {
        return first;
    }

    public void setFirst(A first) {
        this.first = first;
    }

    public B getSecond() {
        return second;
    }

    public void setSecond(B second) {
        this.second = second;
    }

    public C getThird() {
        return third;
    }

    public void setThird(C third) {
        this.third = third;
    }

    public D getFourth() { return fourth;  }

    public void setFourth(D fourth) { this.fourth = fourth; }

    public E getFifth() { return fifth; }

    public void setFifth(E fifth) { this.fifth = fifth; }
}
