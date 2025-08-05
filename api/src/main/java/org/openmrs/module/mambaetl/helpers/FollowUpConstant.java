package org.openmrs.module.mambaetl.helpers;

import org.springframework.util.StringUtils;

import java.util.Arrays;

public class FollowUpConstant {
	
	private static final String ALL = "All";
	
	private static final String ALIVE = "Alive";
	
	private static final String TO = "Transfer out";
	
	private static final String STOP = "Stop";
	
	private static final String LOST = "Lost";
	
	private static final String RESTART = "Restart";
	
	private static final String DROP = "Drop";
	
	private static final String DEAD = "Dead";
	
	public static String getAllListOfStatus() {
		return (ALL + "," + ALIVE + "," + TO + "," + STOP + "," + LOST + "," + RESTART + "," + DROP + "," + DEAD);
	}
	
	public static String getListOfStatus() {
		return ("Alive,Transferred out,Stop all,Loss to follow-up (LTFU),Restart medication,Ran away,Dead");
	}
	
	public static String getDbRepresentation(String status) {
		switch (status) {
			case ALIVE:
				return "Alive";
			case TO:
				return "Transferred out";
			case STOP:
				return "Stop all";
			case LOST:
				return "Loss to follow-up (LTFU)";
			case RESTART:
				return "Restart medication";
			case DROP:
				return "Ran away";
			case DEAD:
				return "Dead";
			default:
				return getListOfStatus();
		}
	}
	
}
