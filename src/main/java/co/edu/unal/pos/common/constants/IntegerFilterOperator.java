package co.edu.unal.pos.common.constants;

public enum IntegerFilterOperator {

	EQUALS,
	LESS,
	MORE,
	LESS_OR_EQUALS,
	MORE_OR_EQUALS;
	
	public static IntegerFilterOperator getFromString(String operator){
		if(operator.equals("<")){
			return LESS;
		}
		if(operator.equals(">")){
			return MORE;
		}
		if(operator.equals("<=")){
			return LESS_OR_EQUALS;
		}
		if(operator.equals(">=")){
			return MORE_OR_EQUALS;
		}
		return EQUALS;
	}
}
