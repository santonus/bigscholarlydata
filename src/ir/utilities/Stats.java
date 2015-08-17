package ir.utilities;


/** A place to put statistical routines
 *
 * @author Ray Mooney
*/

public class Stats
{

    public static double mean(double[] x) {
	double sum = 0;
	for(int i = 0; i < x.length; i++) {
	    sum = sum + x[i];
	}
	return sum/x.length;
    }
    public static double standardDeviation(double[] x) {
	double sum = 0;
	double mean = mean(x);
	for(int i = 0; i < x.length; i++) {
	    sum = sum + Math.pow(x[i]-mean, 2);
	}
	return Math.sqrt(sum/x.length);
    }

    public static double covariance(double[] x, double[] y) {
	if (x.length != y.length) {
	    System.out.println("\nCovariance: Error: Vectors not same length.");
	    System.exit(1);
	}
	double sum = 0;
	double xMean = mean(x);
	double yMean = mean(y);
	for(int i = 0; i < x.length; i++) {
	    sum = sum + (x[i] - xMean)*(y[i] - yMean);
	}
	return sum/x.length;
    }

    public static double pearsonCorrelation(double[] x, double[] y) {
	return covariance(x,y) / (standardDeviation(x) * standardDeviation(y));
    }
	
	    
    public static void main(String[] args) {
	double[] x = {9,3,3,9};
	double[] y = {9,5,1};
	System.out.println(mean(x));
    }
			   

}

