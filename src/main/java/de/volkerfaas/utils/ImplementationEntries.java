package de.volkerfaas.utils;

import java.util.Optional;

public final class ImplementationEntries {

    public static ImplementationEntries get(Class<?> clazz) {
        final Package packageOfClass = clazz.getPackage();
        return new ImplementationEntries(
                packageOfClass.getImplementationTitle(),
                packageOfClass.getImplementationVendor(),
                packageOfClass.getImplementationVersion()
        );
    }

    private final String title;
    private final String vendor;
    private final String version;

    private ImplementationEntries(String title, String vendor, String version) {
        this.title = title;
        this.vendor = vendor;
        this.version = version;
    }

    public String getTitle(String defaultTitle) {
        return Optional.ofNullable(title).orElse(defaultTitle);
    }

    public String getVendor(String defaultVendor) {
        return Optional.ofNullable(vendor).orElse(defaultVendor);
    }

    public String getVersion(String defaultVersion) {
        return Optional.ofNullable(version).orElse(defaultVersion);
    }

}
