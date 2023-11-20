package com.shahidul.csvprocessor.utils;

import java.util.regex.Pattern;

public class ValidationUtils {
    public static final String patterns
            = "^(\\+\\d{1,3}( )?)?((\\(\\d{3}\\))|\\d{3})[- .]?\\d{3}[- .]?\\d{4}$"
            + "|^(\\+\\d{1,3}( )?)?(\\d{3}[ ]?){2}\\d{3}$"
            + "|^(\\+\\d{1,3}( )?)?(\\d{3}[ ]?)(\\d{2}[ ]?){2}\\d{2}$";
    public static final Pattern phoneNumberPattern = Pattern.compile(patterns);

    public static boolean isValidPhone(String phone) {
        return phoneNumberPattern.matcher(phone).matches();
    }
}
