package co.edu.unal.pos.model;

import co.edu.unal.pos.common.constants.IntegerFilterOperator;

public class SaleFactFilter {

	
	private String id;
	
	private String description;
	
	private Integer year;
	private IntegerFilterOperator yearOperator;
	
	private Integer month;
	private IntegerFilterOperator monthOperator;
	
	private Integer day;
	private IntegerFilterOperator dayOperator;
	
	public SaleFactFilter(String id, String description,Integer year, IntegerFilterOperator yearOperator,Integer month, IntegerFilterOperator monthOperator,Integer day, IntegerFilterOperator dayOperator) {
		this.id = id;
		this.description = description;
		this.year = year;
		this.yearOperator = yearOperator;
		this.month = month;
		this.monthOperator = monthOperator;
		this.day = day;
		this.dayOperator = dayOperator;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public IntegerFilterOperator getYearOperator() {
		return yearOperator;
	}

	public void setYearOperator(IntegerFilterOperator yearOperator) {
		this.yearOperator = yearOperator;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}

	public IntegerFilterOperator getMonthOperator() {
		return monthOperator;
	}

	public void setMonthOperator(IntegerFilterOperator monthOperator) {
		this.monthOperator = monthOperator;
	}

	public Integer getDay() {
		return day;
	}

	public void setDay(Integer day) {
		this.day = day;
	}

	public IntegerFilterOperator getDayOperator() {
		return dayOperator;
	}

	public void setDayOperator(IntegerFilterOperator dayOperator) {
		this.dayOperator = dayOperator;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
	
		
}
