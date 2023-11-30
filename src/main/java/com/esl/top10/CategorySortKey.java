package com.esl.top10;

import java.io.Serializable;

public class CategorySortKey implements Comparable<CategorySortKey>, Serializable {
    private int viewCount;  // 查看次数
    private int cartCount;  // 加入购物车次数
    private int purchaseCount;  // 购买次数

    public CategorySortKey(int viewCount, int cartCount, int purchaseCount) {
        this.viewCount = viewCount;
        this.cartCount = cartCount;
        this.purchaseCount = purchaseCount;
    }

    public int getViewCount() {
        return viewCount;
    }

    public void setViewCount(int viewCount) {
        this.viewCount = viewCount;
    }

    public int getCartCount() {
        return cartCount;
    }

    public void setCartCount(int cartCount) {
        this.cartCount = cartCount;
    }

    public int getPurchaseCount() {
        return purchaseCount;
    }

    public void setPurchaseCount(int purchaseCount) {
        this.purchaseCount = purchaseCount;
    }

    public int compareTo(CategorySortKey key){
        if (viewCount-key.getViewCount() != 0)
            return viewCount- key.getViewCount();
        else if (cartCount-key.getCartCount() != 0)
            return cartCount-key.getCartCount();
        else if (purchaseCount- key.getPurchaseCount() != 0)
            return purchaseCount-key.getPurchaseCount();
        return 0;
    }
}
