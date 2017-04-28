import scipy.optimize as o
import scipy.special as sp
from scipy.stats import  ks_2samp
from scipy import stats
import numpy as np
import config as s


def phi(x): return ( sp.erf(1.0 * x/np.sqrt(2)) + 1 ) / 2 

def model_func(t, lamb, mu,sigm):
	ret = 0.0
	ret = s.m*( np.exp(1.0 * lamb * phi( (np.log(t)- mu)/sigm ) ) -1 )
	return ret

def exp_func(t,A,B):
	return A * np.exp( B * t )

def pref_func(t,A,B):
	return (A*t) + B

def quad_func(t,A,B,C):
	return A + (B*t)+(C*(t**2))

def lognormal_func(del_t,mu,sigm,q):
	e = np.exp( (-1.0*((np.log(del_t) - mu)**2)) / (2*(sigm**2)) )
	ret = q*( e  / (np.sqrt(2*np.pi)  * del_t * sigm) )
	return ret

def lognormal_model(t,mu,sigm,q):
	ret = q * phi( (np.log(t)-mu)/sigm )
	return ret
'''	
def gauss_function(x, mu, sigma,a):
    return a*np.exp(-(x-x0)**2/(2*sigma**2))

def normal_func(t,mu,sigm):
	e = np.exp(( -1.0*(t-mu)**2) / (2*(sigm**2)) )
	ret = r / np.sqrt()
'''
def fitmodel(xid,x,y):
	'''
	fits data points into model and returns lamda mu and sigma parameters for paper
	'''
	x = np.array(x)
	y = np.array(y)
	opt_parms,parm_cov = None,None
	try:
		opt_parms, parm_cov = o.curve_fit(model_func, x, y,maxfev=5000)
		lamb , mu, sigm = opt_parms
		#if lamb > 10 or sigm  > 10 or 
	except RuntimeError:
		print "curve fit failed for ",xid
		lamb , mu, sigm = 0, 0 ,0 
	return lamb , mu, sigm, parm_cov	
'''
depricated for now, used to calculate 
'''
def cinf(lamb):
	return m * ( np.exp(lamb) - 1)