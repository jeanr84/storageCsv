package com.earlybirds.storage.extractor.lookup;

import lombok.extern.slf4j.Slf4j;

import static com.earlybirds.storage.schema.InputSchema.ITEM_ID_COLUMN;
import static com.earlybirds.storage.schema.InputSchema.USER_ID_COLUMN;

@Slf4j
public class LookupExtractorFactory {

    public static LookupExtractor create(LookupExtractorEnum type) {
        switch (type) {
            case USER:
                return new LookupExtractor(USER_ID_COLUMN, "src/main/resources/extract/lookup_user.csv", 200);
            case PRODUCT:
                return new LookupExtractor(ITEM_ID_COLUMN, "src/main/resources/extract/lookup_product.csv", 200);
            default:
                log.error("Unexpected type");
                return null;
        }
    }
}
