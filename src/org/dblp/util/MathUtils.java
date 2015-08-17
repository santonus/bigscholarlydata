package org.dblp.util;
import java.util.Arrays;
public class MathUtils {
	public MathUtils() {
		super();
		// TODO Auto-generated constructor stub
	}
	public static final double LOG2 = Math.log(2);
	public static final double ALMOST_ZERO = 0.00001;
	
	/** 
	 * This method normalizes a row by converting the value of every column between 0..1, for each row.
	 * This is done by first identifying max value of the row and then diving value of each column 
	 * of this row by the max value. 
	 * @param input
	 * @return
	 */
	
	public static double[] normalize(double[] input) {
		int colno= input.length;
		double[] normalized = new double[colno];
		Arrays.fill(normalized, 0.0);
		double rowmax= 0.0;
		for (int i = 0; i < colno; i++) {
			if (rowmax < input[i]) {
				rowmax = input[i];
			}
		}
		if (isSignificantDiff(rowmax,0.0)==false)
			return normalized;
		for (int i = 0; i < colno; i++) {
			normalized[i] = input[i]/rowmax;
		}
		return normalized;
	}
	
	/**
	 * Generates a probability distribution (PD) from an input array. 
	 * @param input
	 * @param rowno
	 * @param colno
	 * @return
	 */
	public static double[] generatePD(double[] input) {
		int colno= input.length;
		double[] pd = new double[colno];
		Arrays.fill(pd, 0.0);
		double rowsum= 0.0;
		for (int i = 0; i < colno; i++) {
			rowsum += input[i];
		}
		if (isSignificantDiff(rowsum,0.0)==false)
			return pd;
		for (int i = 0; i < colno; i++) {
			pd[i] = input[i]/rowsum;
		}
		return pd;
	}
	
	/** Computes Kullback–Leibler divergence between two probability distributions pd1 and pd2 represented as arrays.
	 * Kullback, S., and Leibler, R. A., 1951, On information and sufficiency, Annals of Mathematical Statistics 22: 79-86. 
	 * @param probability distribution pd1
	 * @param probability distribution pd2
	 * @return
	 */
	public static double computeKLDistance(double[] pd1, double[] pd2) {
        double kl = 0.0;
        for (int i=0; i < pd1.length; i++) {
			double n = (pd2[i] > 0)? (pd1[i]/pd2[i]) : 0 ;
            double t = (n != 0)? (Math.log(n) / LOG2) : 0 ;
                	kl +=  pd1[i] * t;
        }
        return removePrecision(kl);
	}
	
	
	/** Computes KL distance of given probability distribution dist from uniform probability distribution. 
	 * @param probability distribution dist
	 * @return KL distance
	 */
	public static double computeKLDistanceFromUniformProb(double[] dist) {
        double log_Km = Math.log(dist.length)/LOG2;
        double sum = 0;
        for (int i=0; i < dist.length; i++) {
        	if (dist[i] > 0)
                sum += dist[i] * (Math.log(dist[i])/LOG2);
        }
        return (removePrecision(log_Km + sum));
	}
	
	/** Computes Jensen-Shannon divergence between probability distributions pd1 and pd2.
	 * J. Lin. Divergence measures based on the shannon entropy. IEEE Trans. on Information Theory, 37(1):145--151, January 1991.
	 * @param probability distribution pd1
	 * @param probability distribution pd2
	 * @return JSDivergence
	 */
	public static double computeJSDivergence(double[] pd1, double[] pd2) {
        double[] pdAvg = new double[pd1.length];
        Arrays.fill(pdAvg, 0.0);
        double js = 0.0;
        for (int i=0; i < pd1.length; i++) {
        	pdAvg[i]= (pd1[i]+pd2[i])/2.0;	
        }
        double kl1= computeKLDistance(pd1, pdAvg);
        double kl2= computeKLDistance(pd2, pdAvg);
        js = removePrecision(( kl1 + kl2 )/2.0);
        return js;
	}
	
	
	/** Compute entropy of a given probability distribution pd.
	 * @param probability distribution pd
	 * @return entropy
	 */
	public static double computeEntropy (double[] pd) {
        double entropy = 0.0;
        for (int i=0; i < pd.length; i++) {
        	if (pd[i] > 0.0)
                entropy += pd[i] * (Math.log(pd[i])/LOG2);
        }
        return removePrecision(entropy);
	}
	
	
	/**
	 * Compute euclidian distance between points x and y.
	 * 
	 * @param point x 
	 * @param point y
	 * @return euclidian distance between the two points.
	 */
	public static double computeEuclidianDistance(double[] x, double[] y) {
		double distance=0;
		for (int i=0; i < x.length; i++) {
			double diff= (x[i]-y[i]);
			distance += diff*diff;
		}
		return Math.sqrt(distance);
	}
	
	/** This method computes cosine similarity of two vectors p1 and p2.
	 * @param vector p1
	 * @param vector p2
	 * @return cosine similarity
	 */
	public static double computeCosineSimilarity(double[] p1, double[] p2) {
		double sim=0;
		double dotproduct= 0;
		double magP1=0, magP2=0;
		for (int i=0; i < p1.length; i++) {
			dotproduct += p1[i] * p2[i];
			magP1 += p1[i] * p1[i];
			magP2 += p2[i] * p2[i];
		}
		if (isSignificantDiff((magP1*magP2), ALMOST_ZERO) == true)
				sim = dotproduct/ Math.sqrt(magP1 * magP2);
		else
			sim = 0.0;
		return sim;
	}
	/**
	 * Implementation from http://en.wikipedia.org/wiki/Correlation
	 * @param probability distribution x
	 * @param probability distribution y
	 * @return correlation
	 */
	public static double computeCorrelation(double[] x, double[] y) {
		double sum_sq_x = 0;
		double sum_sq_y = 0;
		double sum_coproduct = 0;
		double mean_x = x[1];
		int N= x.length;
		double mean_y = y[1];
		for (int i = 2 ; i < N; i++) {
		    double sweep = (i - 1.0) / i;
		    double delta_x = x[i] - mean_x;
		    double delta_y = y[i] - mean_y;
		    sum_sq_x += delta_x * delta_x * sweep;
		    sum_sq_y += delta_y * delta_y * sweep;
		    sum_coproduct += delta_x * delta_y * sweep;
		    mean_x += delta_x / i;
		    mean_y += delta_y / i ;
		}
		double pop_sd_x = Math.sqrt( sum_sq_x / N );
		double pop_sd_y = Math.sqrt( sum_sq_y / N );
		double cov_x_y = sum_coproduct / N;
		double correlation = cov_x_y / (pop_sd_x * pop_sd_y);
		return correlation;
	}

	/**
	 * This method asserts that there is no significant difference ( i.e diff < 0.0000001)
	 * between the parameters.
	 * @param d1 first number
	 * @param d2 second number
	 */
	public static final void assertNoSignificantDiff(double d1, double d2) {
		assert (Math.abs(d1 - d2) <= 0.0000001);
	}

	/**
	 * This method asserts that there is no significant difference ( i.e diff < 0.0000001)
	 * between each corresponding element in double arrays d1 and d2.
	 * @param d1 first array of numbers
	 * @param d2 second array of numbers
	 */
	public static final void assertNoSignificantDiff(double[] d1, double[] d2) {
		for (int i = 0; i < d1.length; i++) {
			assert (Math.abs(d1[i] - d2[i]) <= 0.0000001);
		}
	}

	/**
	 * This method asserts that there is no significant difference ( i.e diff < 0.0000001)
	 * between each corresponding element in two dimensional double arrays d1 and d2.
	 * @param d1 first two dimentional array of numbers
	 * @param d2 second two dimentional array of numbers
	 */
	public static void assertNoSignificantDiff(double[][] d1, double[][] d2) {
		for (int i = 0; i < d1.length; i++) {
			for (int j = 0; j < d1[0].length; j++) {
				assert (Math.abs(d1[i][j] - d2[i][j]) <= 0.0000001);
			}
		}
	}

	/**
	 * This method asserts that there is no significant difference ( i.e diff < 0.0000001)
	 * between each corresponding element in two dimensional int arrays d1 and d2.
	 * @param d1 first array of numbers
	 * @param d2 second array of numbers
	 */
	public static void assertNoSignificantDiff(int[] d1, int[] d2) {
		for (int i = 0; i < d1.length; i++) {
			assert (d1[i] == d2[i]);
		}
	}

	/**
	 * This method asserts that there is significant difference ( i.e diff > diff)
	 * between the parameters.
	 * @param d1 first number
	 * @param d2 first number
	 * @param diff difference
	 */
	public static final boolean isSignificantDiff(double d1, double d2, double diff) {
		if (Math.abs(d1 - d2) > diff) {
			return true;
		}
		return false;
	}
	
	/**
	 * This method asserts that there is significant difference ( i.e diff > ALMOST_ZERO )
	 * between the parameters.
	 * @param d1 first number
	 * @param d2 second number
	 * 
	 */
	public static final boolean isSignificantDiff(double d1, double d2) {
//		if (Math.abs(d1 - d2) > 0.0000001) {
		return isSignificantDiff(d1,d2,  ALMOST_ZERO);
	}
	
	
	
	/** Removes precision to 8 digits.
	 * @param d a number
	 * @return number with precision removed
	 */
	public static final double removePrecision(double d) {
		/* return MathUtils.round(d, NDIGITS); */
		return (Math.rint(d * 100000000) / 100000000);
		// return d;
	}
	
	/**
	 * Calculates mean ignoring NaN's.
	 * @param double array numbers
	 * @return mean
	 */
	public static double meanIgnoreNaN(double [] numbers) {
		int count=0;
		double res = 0.0;
		for (double num : numbers) {
			if (Double.isNaN(num) == false) {
				res += num; count++;
			}
		}
		if (count > 0)
			res= res / (double) count;
		else
			res= Double.NaN;
		return res;
	}
	
	/** Calculates mean ignoring NaN as well as zero values.
	 * @param double array numbers
	 * @return mean
	 */
	public static double meanIgnoreNaNZero(double [] numbers) {
		int count=0;
		double res = 0.0;
		for (double num : numbers) {
			if (Double.isNaN(num) == false && num > 0.0) {
				res += num; count++;
			}
		}
		if (count > 0)
			res= res / (double) count;
		else
			res= Double.NaN;
		return res;
	}
	
	/** Calculate Max of a given sequence of numbers ignoring NaN.
	 * @param numbers array of numbers
	 * @return Max
	 */
	public static double maxIgnoreNaN(double [] numbers) {
		double max = Double.MIN_VALUE;
		for (double num : numbers) {
			if (Double.isNaN(num) == false && num > max)
				max = num;
		}
		return max;
	}
	
	/** Calculate Min of a given sequence of numbers ignoring NaN.
	 * @param numbers array of numbers
	 * @return min
	 */
	public static double minIgnoreNaN(double [] numbers) {
		double min = Double.MAX_VALUE;
		for (double num : numbers) {
			if (Double.isNaN(num) == false && num < min)
				min = num;
		}
		return min;
	}
	/** Calculate Max from a given array of numbers and return its index into the array
	 * @param numbers array of numbers
	 * @return index of max element
	 */
	public static int indexForMaxIgnoreNaN(double [] numbers) {
		double max = Double.MIN_VALUE;
		int ndx= -1;
		for (int i=0; i < numbers.length ; i++) {
			if (Double.isNaN(numbers[i]) == false && numbers[i] > max) {
				max = numbers[i];  ndx=i;
			}
		}
		return ndx;
	}
	/**
	 * Calculate Man from a given array of numbers and return its index into the array
	 * @param numbers array of numbers
	 * @return index of min element.
	 */
	public static int indexForMinIgnoreNaN(double [] numbers) {
		double min = Double.MAX_VALUE;
		int ndx= -1;
		for (int i=0; i < numbers.length ; i++) {
			if (Double.isNaN(numbers[i]) == false && numbers[i] < min) {
				min = numbers[i];  ndx=i;
			}
		}
		return ndx;
	}
}
