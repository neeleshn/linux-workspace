import weka.core.*;
import weka.classifiers.*;


public class Main {
	FastVector attributes= new FastVector();
	attributes.addElement(new Attribute("Month"));
	attributes.addElement(new Attribute("DayOfWeek"));
	attributes.addElement(new Attribute("Carrier"));
	attributes.addElement(new Attribute("OriginId"));
	attributes.addElement(new Attribute("DestId"));
	attributes.addElement(new Attribute("DepartureHour"));
	attributes.addElement(new Attribute("Distance"));
	FastVector possibleClasses = new FastVector(2);
	possibleClasses.addElement("0");
	possibleClasses.addElement("1");
	attributes.addElement(new Attribute("Delayed",possibleClasses));
	
	Instances train = new Instances("Train",attributes,0)
}
