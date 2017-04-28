import numpy as np

def rmsef(predictions, targets):
    return np.sqrt(((predictions - targets) ** 2).mean())
def maef(predictions,targets):
    return np.mean(np.abs(targets-predictions))
def mapef(predictions,targets):
    return np.mean(np.abs((targets - predictions) / targets))
