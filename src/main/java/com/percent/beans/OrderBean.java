package com.percent.beans;

/**
 * @author yunpeng.gu
 * @date 2021/6/18 11:16
 * @Email:yunpeng.gu@percent.cn
 */
public class OrderBean {
    private String oid;
    private String uid;
    private double money;
    private double longitude;
    private double latitude;
    private String province;
    private String city;
    private  String district;

    public OrderBean() {
    }

    public OrderBean(String oid, String uid, double money, double longitude, double latitude, String province,
                     String city, String district) {
        this.oid = oid;
        this.uid = uid;
        this.money = money;
        this.longitude = longitude;
        this.latitude = latitude;
        this.province = province;
        this.city = city;
        this.district = district;
    }

    public String getOid() {
        return oid;
    }
    public void setOid(String oid) {
        this.oid = oid;
    }
    public String getUid() {
        return uid;
    }
    public void setUid(String uid) {
        this.uid = uid;
    }
    public double getMoney() {
        return money;
    }
    public void setMoney(double money) {
        this.money = money;
    }
    public double getLongitude() {
        return longitude;
    }
    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
    public double getLatitude() {
        return latitude;
    }
    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }
    public String getProvince() {
        return province;
    }
    public void setProvince(String province) {
        this.province = province;
    }
    public String getCity() {
        return city;
    }
    public void setCity(String city) {
        this.city = city;
    }
    public String getDistrict() {
        return district;
    }
    public void setDistrict(String district) {
        this.district = district;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "oid='" + oid + '\'' +
                ", uid='" + uid + '\'' +
                ", money=" + money +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", district='" + district + '\'' +
                '}';
    }
}

