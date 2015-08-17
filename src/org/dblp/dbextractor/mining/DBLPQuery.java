package org.dblp.dbextractor.mining;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.dblp.dbextractor.DBLPVenue;

import com.csvreader.CsvWriter;

public abstract class DBLPQuery {

	protected int earliestY;
	protected int timeSlotBegin;
	protected int timeSlotLast;
	protected int latestY;
	protected int timestep;
	protected int currResearchDomainId;
	protected String currResearchDomainDB;
	public final void setTimeStep(int time) {
		timestep= time;
	}
	public final void setBeginEndYear(int begin, int end) {
		timeSlotBegin= begin;
		timeSlotLast= end;
	}
	public final void setEarliestLatestYear(int e, int l) {
		earliestY= e;
		latestY= l;
	}
	public final void setResearchDomain(int id) {
		currResearchDomainId= id;
		currResearchDomainDB = DBLPVenue.getResearchDomainViewName(id);
	}
	abstract public void processQuery(Connection conn, CsvWriter outfile) throws SQLException, IOException;
}
