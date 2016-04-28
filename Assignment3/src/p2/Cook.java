package p2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * Cooks are simulation actors that have at least one field, a name.
 * When running, a cook attempts to retrieve outstanding orders placed
 * by Eaters and process them.
 */
public class Cook implements Runnable {
	private final String name;

	/**
	 * You can feel free modify this constructor.  It must
	 * take at least the name, but may take other parameters
	 * if you would find adding them useful. 
	 *
	 * @param: the name of the cook
	 */
	public Cook(String name) {
		this.name = name;
	}

	public String toString() {
		return name;
	}

	/**
	 * This method executes as follows.  The cook tries to retrieve
	 * orders placed by Customers.  For each order, a List<Food>, the
	 * cook submits each Food item in the List to an appropriate
	 * Machine, by calling makeFood().  Once all machines have
	 * produced the desired Food, the order is complete, and the Customer
	 * is notified.  The cook can then go to process the next order.
	 * If during its execution the cook is interrupted (i.e., some
	 * other thread calls the interrupt() method on it, which could
	 * raise InterruptedException if the cook is blocking), then it
	 * terminates.
	 */
	public void run() {

	    
	    
		Simulation.logEvent(SimulationEvent.cookStarting(this));
			while(true) {
			    Customer customer = null;
    			try{    
    			    customer = Simulation.priorityCustomerBlockingQueue.take();
    			} catch (InterruptedException e) {
                    Simulation.logEvent(SimulationEvent.cookEnding(this));
                    return;
                }
    			
    			List<FutureTask<Food>> currentlyCookingFoodList = null;
                if (customer != null) {
                    List<Food> foodList = customer.getOrder();

                    Simulation.logEvent(SimulationEvent.cookReceivedOrder(this, foodList, customer.getOrderNum()));
                    
                    currentlyCookingFoodList = new ArrayList<FutureTask<Food>>();
                    
                    for(Food food:foodList){
                        Machine currentMachine = null;

                        if(food.name.equals("burger")){
                            currentMachine = Simulation.burgerMachine;
                        }
                        else if(food.name.equals("fries")){
                            currentMachine = Simulation.friesMachine;
                        }
                        else if(food.name.equals("coffee")){
                            currentMachine = Simulation.coffeeMachine;
                        }

                        try {
                            currentMachine.machineCap.acquire();
                        } catch (InterruptedException e) {
                            System.out.println("Cook interrupted while waiting for oven space");
                        }
                        

                        Simulation.logEvent(SimulationEvent.cookStartedFood(this, food, customer.getOrderNum()));
                        try {
                            currentlyCookingFoodList.add(currentMachine.makeFood());
                        } catch (InterruptedException e) {
                            System.out.println("Cook interrupted while doing makeFood");
                        }
                        
                    }
                }
                
                int i = 0;
                
                List<Integer> cookedAndVerifiedList = new ArrayList<Integer>();
                
                int size = currentlyCookingFoodList.size();
                Food cooked = null;
                
                while (cookedAndVerifiedList.size() < size) {
                    
                    if (!cookedAndVerifiedList.contains(new Integer(i % size))) {
                    
                        if (currentlyCookingFoodList.get(i % size).isDone()) {
                            try {
                                cooked = currentlyCookingFoodList.get(i % size).get();
                            } catch (InterruptedException e) {
                                System.out.println("Cook stopped during checking status");
                            } catch (ExecutionException ex) {
                                System.out.println("Cook stopped during checking status");
                            } 
                            
                            Machine currentMachine = null;

                            if(cooked.name.equals("burger")){
                                currentMachine = Simulation.burgerMachine;
                            }
                            else if(cooked.name.equals("fries")){
                                currentMachine = Simulation.friesMachine;
                            }
                            else if(cooked.name.equals("coffee")){
                                currentMachine = Simulation.coffeeMachine;
                            }
                            
                            Simulation.logEvent(SimulationEvent.machineDoneFood(currentMachine, cooked));
                            customer.getCountDownLatch().countDown();
                            
                            Simulation.logEvent(SimulationEvent.cookFinishedFood(this, cooked, customer.getOrderNum()));
                            currentMachine.machineCap.release();
                            
                            cookedAndVerifiedList.add(new Integer(i));
                        }
                    }
                    
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        System.out.println("Cook interrupted while sleeping");
                    }
                    i++;
                }
                
                // Now that the order is complete. Decrement the CountDownLatch one more time to signal
                // the Customer to take his food
                Simulation.logEvent(SimulationEvent.cookCompletedOrder(this, customer.getOrderNum()));
                    				
    	}
		
	}
}