package com.atif.kafka.producer;

public class UserProperties {
	final static String[] stateList = {"AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
			"HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN",
			"MS","MO","MT","NE","NV","OH","OK","OR","PA","TN","TX","UT","VT",
			"VA","WA","WI","WY","NH","NJ","NM","NY","NC","ND","RI","SC","SD",
			"WV"
		};
	
	final static String[] segmentList = {"Younger", "Middle Aged", "Older"};

	public static String[] getStateList() {
		return stateList;
	}

	public static String[] getSegmentList() {
		return segmentList;
	}
}

