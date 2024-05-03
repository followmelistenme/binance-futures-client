package binancefuturesclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"go.uber.org/zap"
)

type FuturesClient struct {
	APIKey     string
	SecretKey  string
	BaseURL    string
	HTTPClient *http.Client
	Logger     *zap.Logger
}

func NewFuturesClient(APIKey, secretKey, baseUrl string, logger *zap.Logger) *FuturesClient {
	return &FuturesClient{APIKey: APIKey, SecretKey: secretKey, BaseURL: baseUrl, Logger: logger, HTTPClient: http.DefaultClient}
}

type Candle struct {
	Symbol     string
	OpenTime   time.Time
	CloseTime  time.Time
	OpenPrice  float64
	ClosePrice float64
	HighPrice  float64
	LowPrice   float64
	Volume     float64
}

func NewCandle(symbol string, item []interface{}) (*Candle, error) {
	parseTimeFn := func(val interface{}) (time.Time, error) {
		floatVal, ok := val.(float64)
		if !ok {
			return time.Now(), fmt.Errorf("couldn't parse time, %v", val)
		}

		return time.UnixMilli(int64(floatVal)), nil
	}

	parseFloatFn := func(val interface{}) (float64, error) {
		stringVal, ok := val.(string)
		if !ok {
			return 0., fmt.Errorf("couldn't parse float64, %v", val)
		}

		f, err := strconv.ParseFloat(stringVal, 64)
		if err != nil {
			return 0., fmt.Errorf("couldn't parse float64, %v, err: %v", val, err)
		}

		return f, nil
	}

	openTime, err := parseTimeFn(item[0])
	if err != nil {
		return nil, err
	}

	closeTime, err := parseTimeFn(item[6])
	if err != nil {
		return nil, err
	}

	openPrice, err := parseFloatFn(item[1])
	if err != nil {
		return nil, err
	}

	closePrice, err := parseFloatFn(item[4])
	if err != nil {
		return nil, err
	}

	highPrice, err := parseFloatFn(item[2])
	if err != nil {
		return nil, err
	}

	lowPrice, err := parseFloatFn(item[3])
	if err != nil {
		return nil, err
	}

	volume, err := parseFloatFn(item[5])
	if err != nil {
		return nil, err
	}

	return &Candle{symbol, openTime, closeTime, openPrice, closePrice, highPrice, lowPrice, volume}, nil
}

func (fc *FuturesClient) LoadCandles(symbol string, params map[string]string) ([]*Candle, error) {
	queryString := ""
	for key, val := range params {
		queryString = fmt.Sprintf("%s&%s=%s", queryString, key, val)
	}
	data, _ := fc.get(context.Background(), fmt.Sprintf("/fapi/v1/klines?symbol=%s%s", symbol, queryString))
	var results [][]interface{}
	candles := []*Candle{}
	err := json.Unmarshal(data, &results)
	if err != nil {
		return nil, err
	}

	for _, item := range results {
		cndl, err := NewCandle(symbol, item)
		if err != nil {
			return nil, err
		}
		candles = append(candles, cndl)
	}

	return candles, nil
}

func (fc *FuturesClient) get(ctx context.Context, path string) (data []byte, err error) {
	fullURL := fmt.Sprintf("%s%s", fc.BaseURL, path)
	req, err := http.NewRequest("GET", fullURL, &bytes.Buffer{})
	if err != nil {
		return []byte{}, err
	}
	req = req.WithContext(ctx)

	header := http.Header{}
	header.Set("User-Agent", fmt.Sprintf("%s/%s", "followmelistenme/binance-futures-client", "0.0.1"))
	header.Set("X-MBX-APIKEY", fc.APIKey)
	req.Header = header

	f := fc.HTTPClient.Do
	res, err := f(req)
	if err != nil {
		return []byte{}, err
	}
	data, err = io.ReadAll(res.Body)
	if err != nil {
		return []byte{}, err
	}
	defer func() {
		cerr := res.Body.Close()
		if err == nil && cerr != nil {
			err = cerr
		}
	}()

	if res.StatusCode >= http.StatusBadRequest {
		var errorString string
		err := json.Unmarshal(data, &errorString)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}
