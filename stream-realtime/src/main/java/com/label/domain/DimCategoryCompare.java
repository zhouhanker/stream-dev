package com.label.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.label.domain.DimCategoryCompare
 * @Author zhou.han
 * @Date 2025/5/14 10:57
 * @description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimCategoryCompare {
    private Integer id;
    private String categoryName;
    private String searchCategory;
}
