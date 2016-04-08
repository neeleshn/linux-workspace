import org.tartarus.snowball.ext.englishStemmer;

public class Main{
	public static void main(String[] args){
		englishStemmer stemmer = new englishStemmer();
		stemmer.setCurrent("doing");
		if(stemmer.stem()) {
			System.out.println("-"+stemmer.getCurrent());
		} else {
			System.out.println(stemmer.getCurrent());
		}
	}
}
