package com.hadoop.migration.config;

public class MetadataConfig {
    private boolean migrateViews = false;
    private boolean generateViewDdl = true;
    private boolean warnOnView = true;
    private String unsupportedAction = "SKIP";
    private boolean rewriteLocations = true;
    private boolean addTransactionalProp = true;
    private boolean clearStatistics = true;
    private boolean upgradeBucketingVersion = true;
    private boolean autoConvert = false;

    public boolean isAutoConvert() { return autoConvert; }
    public void setAutoConvert(boolean autoConvert) { this.autoConvert = autoConvert; }

    public boolean isMigrateViews() { return migrateViews; }
    public void setMigrateViews(boolean migrateViews) { this.migrateViews = migrateViews; }

    public boolean isGenerateViewDdl() { return generateViewDdl; }
    public void setGenerateViewDdl(boolean generateViewDdl) { this.generateViewDdl = generateViewDdl; }

    public boolean isWarnOnView() { return warnOnView; }
    public void setWarnOnView(boolean warnOnView) { this.warnOnView = warnOnView; }

    public String getUnsupportedAction() { return unsupportedAction; }
    public void setUnsupportedAction(String unsupportedAction) { this.unsupportedAction = unsupportedAction; }

    public boolean isRewriteLocations() { return rewriteLocations; }
    public void setRewriteLocations(boolean rewriteLocations) { this.rewriteLocations = rewriteLocations; }

    public boolean isAddTransactionalProp() { return addTransactionalProp; }
    public void setAddTransactionalProp(boolean addTransactionalProp) { this.addTransactionalProp = addTransactionalProp; }

    public boolean isClearStatistics() { return clearStatistics; }
    public void setClearStatistics(boolean clearStatistics) { this.clearStatistics = clearStatistics; }

    public boolean isUpgradeBucketingVersion() { return upgradeBucketingVersion; }
    public void setUpgradeBucketingVersion(boolean upgradeBucketingVersion) { this.upgradeBucketingVersion = upgradeBucketingVersion; }
}
