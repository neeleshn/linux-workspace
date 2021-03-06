package p2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import p2.SimulationEvent;


/**
 * Validates a simulation
 */
public class Validate {
    private static class InvalidSimulationException extends Exception {
        public InvalidSimulationException() { }
    };

    // Helper method for validating the simulation
    private static void check(boolean check,
            String message) throws InvalidSimulationException {
        if (!check) {
            System.err.println("SIMULATION INVALID : "+message);
            throw new Validate.InvalidSimulationException();
        }
    }

    /** 
     * Validates the given list of events is a valid simulation.
     * Returns true if the simulation is valid, false otherwise.
     *
     * @param events - a list of events generated by the simulation
     *   in the order they were generated.
     *
     * @returns res - whether the simulation was valid or not
     */
    public static boolean validateSimulation(List<SimulationEvent> events) {
        boolean valid = true;
                try {
            check(events.get(0).event == SimulationEvent.EventType.SimulationStarting,
                    "Simulation didn't start with initiation event");
            check(events.get(events.size()-1).event == 
                    SimulationEvent.EventType.SimulationEnded,
                    "Simulation didn't end with termination event");

            /* In P2 you will write validation code for things such as:
                Should not have more customers than specified
                Should not have more cooks than specified
                The coffee shop capacity should not be exceeded
                The capacity of each machine should not be exceeded
                customer should not receive order until cook completes it
                customer should not leave coffee shop until order is received
                customer should not place more than one order
                Cook should not work on order before it is placed
             */

                        // Now check if there is the correct number of customers entering and leaving the coffee shop. Also check
                        // if the coffee shop is ever filled above capacity
                        
                        int customers_entered = 0;
                        int customers_left = 0;
                        int customers_sitting = 0;
                        
                        Map<Customer, Integer> customers = new HashMap<Customer, Integer>();
                        Map<Customer, Integer> customers_order = new HashMap<Customer, Integer>();
                        for (SimulationEvent e : events) {
                            check(customers_sitting <= events.get(0).simParams[2], "There are more customers sitting then there are tables");
                                if (e.event == SimulationEvent.EventType.CustomerEnteredCoffeeShop) {
                                    customers_entered++;
                                    customers_sitting++;
                                // 0 represents no order placed in this map. 1 represents order placed. 2 represents order completed
                                customers.put(e.customer, 0);
                                // -1 means no order placed yet. Otherwise it is the order number
                                customers_order.put(e.customer, -1);
                                }
                                if (e.event == SimulationEvent.EventType.CustomerLeavingCoffeeShop) {
                                        customers_left++;
                                        customers_sitting--;
                                }
                        }
                        check(customers_entered == customers_left, "customers entered does not equal customers left");
                        check (customers_entered == events.get(0).simParams[0], "customers entered does not equal " + events.get(0).simParams[0]);
            
                        
                        
                        // Check if every customer received the correct order and exactly one order. Check if orders are placed by customers in the coffee shop
                        for (SimulationEvent e1 : events) {
                            if (e1.event == SimulationEvent.EventType.CustomerPlacedOrder) {
                                check(customers.get(e1.customer) != null, "An order was placed by an customer who did not enter the coffee shop");
                                customers.put(e1.customer, customers.get(e1.customer) + 1);
                                customers_order.put(e1.customer, e1.orderNumber);
                            }
                        }
                
                        for (SimulationEvent e1 : events) {
                            if (e1.event == SimulationEvent.EventType.CustomerReceivedOrder) {
                                check(customers.get(e1.customer) != null, "An order was received by an customer who did not enter the coffee shop");
                                check(customers_order.get(e1.customer) == e1.orderNumber, "An customer received the wrong order");
                                customers.put(e1.customer, customers.get(e1.customer) + 1);
                            }
                        }
            
                        for (Integer i : customers.values()) {
                            check(i == 2, "There exists an customer who did not place and receive and order.");
                        }

                        
                        // Check the right amount of machines start and end
                        int machines_started = 0;
                        int machines_finished = 0;
            
                        Map<Machine, Integer> machine_current_capacity = new HashMap<Machine, Integer>();
                        for (SimulationEvent e2 : events) {
                            if (e2.event == SimulationEvent.EventType.MachineStarting) {
                                machines_started++;
                                machine_current_capacity.put(e2.machine, 0);
                            }
                
                            if (e2.event == SimulationEvent.EventType.MachineEnding) {
                                machines_finished++;
                            }
                        }
                        check(machines_started == machines_finished, "Machines started does not equal machines finished");
                        check(machines_started == 3, "machines started does not equal 3");


                        // Check machines never go above or below capacity. Check that all food that is finished is started and vice-versa
                        // Check that machines always cook valid foods
                        for (SimulationEvent e3 : events) {
                            if (e3.event == SimulationEvent.EventType.MachineStartingFood) {
                                check(machine_current_capacity.get(e3.machine) != null, "Machine started food that wasn't initialized");
                                machine_current_capacity.put(e3.machine, machine_current_capacity.get(e3.machine) + 1);
                                check(machine_current_capacity.get(e3.machine) <= events.get(0).simParams[3], "Machine is over filled");
                                check(e3.machine.machineFoodType == e3.food, "Machine started cooking invalid food type");
                            } 
                            if (e3.event == SimulationEvent.EventType.MachineDoneFood) {
                                check(machine_current_capacity.get(e3.machine) != null, "Machine finished food that wasn't initialized");
                                machine_current_capacity.put(e3.machine, machine_current_capacity.get(e3.machine) - 1);
                                check(machine_current_capacity.get(e3.machine) >= 0, "Machine is under filled");
                                check(e3.machine.machineFoodType == e3.food, "Machine finished cooking invalid food type");
                            }   
                        }

                        
                        // Now check if there is the correct number of cooks entering and leaving the coffee shop
                        int cooks_entered = 0;
                        int cooks_left = 0;
                        
                        for (SimulationEvent e : events) {
                        if (e.event == SimulationEvent.EventType.CookStarting) 
                            cooks_entered++;
                        if (e.event == SimulationEvent.EventType.CookEnding) 
                            cooks_left++;
                        }
                        
                        check(cooks_entered == cooks_left, "Cooks entered does not equal customers left");
                        check (cooks_entered == events.get(0).simParams[1], "Cooks entered does not equal " + events.get(0).simParams[1]);
            
        
        
            //return true;
        } catch (InvalidSimulationException e) {
        valid = false;
        }
                return valid;
    }
        
}
