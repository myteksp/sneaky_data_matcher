package com.dataprocessor.server.services;

import com.dataprocessor.server.utils.StringTransformer;
import com.dataprocessor.server.utils.csv.CsvUtil;
import org.apache.commons.validator.routines.EmailValidator;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public final class RecordValidationUtilService {
    public final String extractAndValidate(final CsvUtil.CsvRecord record,
                                            final String rowName,
                                            final List<StringTransformer.Transformation> transformations){
        final String rowNameLowerCase = rowName.toLowerCase();
        final String result = StringTransformer.transform(record.getColumnVale(rowName), transformations);
        if (rowNameLowerCase.contains("mail")){
            if (EmailValidator.getInstance(true, true).isValid(result)){
                return result;
            }else {
                return "";
            }
        }
        if (rowNameLowerCase.contains("phone")){
            final String onlyNumber = result.replaceAll("[^\\d.]", "").replace('.', ' ').replace(" ", "");
            if (onlyNumber.length() < 5){
                return "";
            }else{
                return onlyNumber;
            }
        }
        return result;
    }
}
