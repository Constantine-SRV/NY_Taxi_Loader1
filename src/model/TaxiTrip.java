package model;

import java.time.LocalDateTime;

/**
 * Модель данных для одной поездки такси NYC.
 * Содержит основные поля из Parquet файла.
 */
public class TaxiTrip {

    private int vendorId;
    private LocalDateTime pickupDatetime;
    private LocalDateTime dropoffDatetime;
    private int passengerCount;
    private double tripDistance;
    private int rateCodeId;
    private String storeAndFwdFlag;
    private int puLocationId;
    private int doLocationId;
    private int paymentType;
    private double fareAmount;
    private double extra;
    private double mtaTax;
    private double tipAmount;
    private double tollsAmount;
    private double improvementSurcharge;
    private double totalAmount;
    private double congestionSurcharge;

    // Конструктор по умолчанию
    public TaxiTrip() {}

    // Геттеры и сеттеры
    public int getVendorId() { return vendorId; }
    public void setVendorId(int vendorId) { this.vendorId = vendorId; }

    public LocalDateTime getPickupDatetime() { return pickupDatetime; }
    public void setPickupDatetime(LocalDateTime pickupDatetime) {
        this.pickupDatetime = pickupDatetime;
    }

    public LocalDateTime getDropoffDatetime() { return dropoffDatetime; }
    public void setDropoffDatetime(LocalDateTime dropoffDatetime) {
        this.dropoffDatetime = dropoffDatetime;
    }

    public int getPassengerCount() { return passengerCount; }
    public void setPassengerCount(int passengerCount) {
        this.passengerCount = passengerCount;
    }

    public double getTripDistance() { return tripDistance; }
    public void setTripDistance(double tripDistance) {
        this.tripDistance = tripDistance;
    }

    public int getRateCodeId() { return rateCodeId; }
    public void setRateCodeId(int rateCodeId) { this.rateCodeId = rateCodeId; }

    public String getStoreAndFwdFlag() { return storeAndFwdFlag; }
    public void setStoreAndFwdFlag(String flag) { this.storeAndFwdFlag = flag; }

    public int getPuLocationId() { return puLocationId; }
    public void setPuLocationId(int id) { this.puLocationId = id; }

    public int getDoLocationId() { return doLocationId; }
    public void setDoLocationId(int id) { this.doLocationId = id; }

    public int getPaymentType() { return paymentType; }
    public void setPaymentType(int paymentType) { this.paymentType = paymentType; }

    public double getFareAmount() { return fareAmount; }
    public void setFareAmount(double fareAmount) { this.fareAmount = fareAmount; }

    public double getExtra() { return extra; }
    public void setExtra(double extra) { this.extra = extra; }

    public double getMtaTax() { return mtaTax; }
    public void setMtaTax(double mtaTax) { this.mtaTax = mtaTax; }

    public double getTipAmount() { return tipAmount; }
    public void setTipAmount(double tipAmount) { this.tipAmount = tipAmount; }

    public double getTollsAmount() { return tollsAmount; }
    public void setTollsAmount(double tollsAmount) { this.tollsAmount = tollsAmount; }

    public double getImprovementSurcharge() { return improvementSurcharge; }
    public void setImprovementSurcharge(double s) { this.improvementSurcharge = s; }

    public double getTotalAmount() { return totalAmount; }
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }

    public double getCongestionSurcharge() { return congestionSurcharge; }
    public void setCongestionSurcharge(double s) { this.congestionSurcharge = s; }

    @Override
    public String toString() {
        return String.format(
                "TaxiTrip{pickup=%s, distance=%.2f mi, fare=$%.2f, tip=$%.2f, total=$%.2f, passengers=%d}",
                pickupDatetime, tripDistance, fareAmount, tipAmount, totalAmount, passengerCount
        );
    }
}