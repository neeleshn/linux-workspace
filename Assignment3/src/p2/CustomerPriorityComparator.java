package p2;

import java.util.Comparator;

public class CustomerPriorityComparator implements Comparator<Customer> {

    @Override
    public int compare(Customer c1, Customer c2) {
            if(c1.getPriority() < c2.getPriority()){
                return 1;
            }
            else if(c1.getPriority() > c2.getPriority()){
                return -1;
            }
        return 0;
    }

}
