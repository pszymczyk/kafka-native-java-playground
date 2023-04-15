package com.pszymczyk.step9;

import java.util.Set;

public interface DebtorsRepository {

    default Set<String> getDebtors() {
        return Set.of(
            "jan k.",
            "wincenty f.",
            "zenon j.",
            "amanda .d",
            "teresa b.",
            "anna g."
        );
    }

    default Set<String> getBlackList() {
        return Set.of(
            "andrzej k.",
            "jacek b.",
            "marzena j.",
            "pawel s.",
            "lucyna j.",
            "waldemar g."
        );
    }
}
