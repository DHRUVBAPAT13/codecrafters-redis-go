package main

import "math"

const (
	geoMinLat = -85.051128278
	geoMaxLat = 85.05112878
	geoMinLon = -180.0
	geoMaxLon = 180.0

	geoLatRange = geoMaxLat - geoMinLat
	geoLonRange = geoMaxLon - geoMinLon
)

func spreadInt32toInt64(v uint32) uint64 {

	result := uint64(v)
	result = (result | (result << 16)) & 0x0000ffff0000ffff
	result = (result | (result << 8)) & 0x00ff00ff00ff00ff
	result = (result | (result << 4)) & 0x0f0f0f0f0f0f0f0f
	result = (result | (result << 2)) & 0x3333333333333333
	result = (result | (result << 1)) & 0x5555555555555555

	return result
}

func interLeave(x, y uint32) uint64 {

	xSpread := spreadInt32toInt64(x)
	ySpread := spreadInt32toInt64(y)
	yShifted := ySpread << 1

	return xSpread | yShifted
}

func encodeGeo(latitude, longitude float64) uint64 {

	normalizedLat := math.Pow(2, 26) * (latitude - geoMinLat) / geoLatRange
	normalizedLon := math.Pow(2, 26) * (longitude - geoMinLon) / geoLonRange

	latInt := uint32(normalizedLat)
	lonInt := uint32(normalizedLon)

	return interLeave(latInt, lonInt)
}

func compactInt64toInt32(v uint64) uint32 {

	result := v & 0x5555555555555555
	result = (result | (result >> 1)) & 0x3333333333333333
	result = (result | (result >> 2)) & 0x0f0f0f0f0f0f0f0f
	result = (result | (result >> 4)) & 0x00ff00ff00ff00ff
	result = (result | (result >> 8)) & 0x0000ffff0000ffff
	result = (result | (result >> 16)) & 0x00000000ffffffff

	return uint32(result)
}

type Coordinates struct {
	Latitude  float64
	Longitude float64
}

func decodeGeo(geoCode uint64) Coordinates {

	x := geoCode >> 1
	y := geoCode

	gridLatNumber := compactInt64toInt32(x)
	gridLonNumber := compactInt64toInt32(y)

	gridLatMin := geoMinLat + geoLatRange*(float64(gridLatNumber)/math.Pow(2, 26))
	gridLatMax := geoMinLat + geoLatRange*(float64(gridLatNumber+1)/math.Pow(2, 26))
	gridLonMin := geoMinLon + geoLonRange*(float64(gridLonNumber)/math.Pow(2, 26))
	gridLonMax := geoMinLon + geoLonRange*(float64(gridLonNumber+1)/math.Pow(2, 26))

	latitude := (gridLatMin + gridLatMax) / 2
	longitude := (gridLonMin + gridLonMax) / 2

	return Coordinates{Latitude: latitude, Longitude: longitude}
}
