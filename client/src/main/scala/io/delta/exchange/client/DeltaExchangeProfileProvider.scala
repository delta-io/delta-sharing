package io.delta.exchange.client

trait DeltaExchangeProfileProvider {
  def getProfile: DeltaExchangeProfile
}

class BasicDeltaExchangeProfileProvider(profile: DeltaExchangeProfile) {
  def getProfile: DeltaExchangeProfile = profile
}