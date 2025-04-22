package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum TxCurrPvlsAggregationTypes {
    NUMERATOR("NUMERATOR"),DENOMINATOR("DENOMINATOR"),BREAST_FEEDING("BREAST_FEEDING"),
    PREGNANT("PREGNANT"),UNKNOWN("UNKNOWN");
    private final String sqlValue;

    TxCurrPvlsAggregationTypes(String sqlValue) {
        this.sqlValue = sqlValue;
    }
    public String getSqlValue() {
        return sqlValue;
    }

    public static TxCurrPvlsAggregationTypes fromString(String status) {
        for (TxCurrPvlsAggregationTypes cs : TxCurrPvlsAggregationTypes.values()) {
            if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
                return cs;
            }
        }
        return UNKNOWN;
    }

}
