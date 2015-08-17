package org.dblp.util;
import java.util.Random;


/**
 * Given a discrete probability distribution over a finite set of elements, this class generates a random number
 * that serves as index to an element.
 * @author santonu_sarkar
 */
public class RandomGenerator {
	private double[] _distribution;
	private double[] _probsum;
	private Random _uniformDistr;
	public RandomGenerator(double []dist) {
		_distribution= dist.clone();
		_probsum= new double[dist.length];
		double sum=0;
		for (int j=0; j< _probsum.length; j++) {
			sum += _distribution[j];
			_probsum[j]= sum;
		}
		_uniformDistr= new Random();
	}
	
	/**
	 * This method selects the index of an element randomly following the given
	 * probability distribution (the distribution with which this object has been
	 * created)
	 * @return int- the index of the element to be picked up
	 */
	public int getNextElementNdx() {
		double r= _uniformDistr.nextDouble();
		for (int j=0; j< _probsum.length-1; j++) {
			if (r <= _probsum[j]) {
				return j;
			}
		}
		return _probsum.length-1;
	}
	
	public int[] generateElementSequence(int length) {
		int[] sequence= new int[length];
		for (int i=0; i < length; i++)
			sequence[i]= getNextElementNdx();
		return sequence;
	}

}
