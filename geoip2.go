package main

import (
	"fmt"
	"github.com/oschwald/geoip2-golang"
	"net"
)

// config json
// {
// "TagToFilter":"syslogtag",
// "ipdb":"(.*)"
// }

type GeoIP2Filter struct {
	TagToFilter string `json:"TagToFilter"`
	db          *geoip2.Reader
}

// todo add function to auto update database from remote addr
// example: download from s3
func NewGeoIP2Filter(config map[string]string) *GeoIP2Filter {
	rf := &GeoIP2Filter{
		TagToFilter: config["TagToFilter"],
	}
	var err error
	rf.db, err = geoip2.Open(config["DataBase"])
	if err != nil {
		fmt.Println(err)
	}
	return rf
}

func (geo *GeoIP2Filter) Cleanup() {
	geo.db.Close()
}

/*
   "geoip" : {
     "country_code2" : "CN",
     "ip" : "180.165.120.8",
     "country_code3" : "CN",
     "latitude" : 31.0456,
     "timezone" : "Asia/Shanghai",
     "continent_code" : "AS",
     "country_name" : "China",
     "region_name" : "Shanghai",
     "city_name" : "Shanghai",
     "location" : {
       "lon" : 121.3997,
       "lat" : 31.0456
     },
     "region_code" : "31",
     "longitude" : 121.3997
   },
*/
func (geo *GeoIP2Filter) Handle(msg *map[string]interface{}) (*map[string]interface{}, error) {
	message, ok := (*msg)[geo.TagToFilter].(string)
	if !ok {
		return msg, fmt.Errorf("bad data format, not a string")
	}
	ip := net.ParseIP(message)
	record, err := geo.db.City(ip)
	if err != nil {
		return msg, err
	}
	rst := make(map[string]interface{})
	rst["city_name"] = record.City.Names["en"]
	//rst["region_name"] = record.City.Names["en"]
	//rst["region_code"] = record.City.Names["code"]
	rst["country_name"] = record.Country.Names["en"]
	rst["country_code2"] = record.RepresentedCountry.IsoCode
	rst["country_code3"] = record.RegisteredCountry.IsoCode
	rst["ip"] = message
	rst["continent_code"] = record.Continent.Code
	rst["timezone"] = record.Location.TimeZone
	rst["latitude"] = record.Location.Latitude
	rst["longitude"] = record.Location.Longitude

	geoinfo := make(map[string]float64)
	geoinfo["lat"] = record.Location.Latitude
	geoinfo["lon"] = record.Location.Longitude
	rst["location"] = geoinfo
	(*msg)["geoip"] = rst
	return msg, nil
}
