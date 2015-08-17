package org.dblpmining;

public class MeasureTime {
    private long _start = 0;
    private long _stop = 0;
    private boolean _isRunning = false;

    
    public void start() {
        this._start = System.currentTimeMillis();
        this._isRunning = true;
    }
    
    public void stop() {
        this._stop = System.currentTimeMillis();
        this._isRunning = false;
    }

    public long getElapsedTime() {
        long elapsed;
        if (_isRunning) {
             elapsed = (System.currentTimeMillis() - _start);
        }
        else {
            elapsed = (_stop - _start);
        }
        return elapsed;
    }

    public long getElapsedTimeSecs() {
        long elapsed;
        if (_isRunning) {
            elapsed = ((System.currentTimeMillis() - _start) / 1000);
        }
        else {
            elapsed = ((_stop - _start) / 1000);
        }
        return elapsed;
    }
}
