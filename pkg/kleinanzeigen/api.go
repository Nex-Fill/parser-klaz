package kleinanzeigen

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var categoryIDRegex = regexp.MustCompile(`/s-cat/(\d+)`)
var adIDRegex = regexp.MustCompile(`/(\d+)-\d+-\d+$`)

func ExtractCategoryID(rawURL string) string {
	if m := categoryIDRegex.FindStringSubmatch(rawURL); len(m) > 1 {
		return m[1]
	}
	return ""
}

func ExtractAdID(rawURL string) string {
	if m := adIDRegex.FindStringSubmatch(rawURL); len(m) > 1 {
		return m[1]
	}
	return ""
}

func BuildSearchURL(params SearchParams) string {
	base := "https://api.kleinanzeigen.de/api/ads.json"
	q := url.Values{}
	q.Set("categoryId", params.CategoryID)
	q.Set("adStatus", params.AdStatus)
	q.Set("page", fmt.Sprintf("%d", params.Page))
	q.Set("size", fmt.Sprintf("%d", params.Size))
	if params.PriceType != "" {
		q.Set("priceType", params.PriceType)
	}
	if params.MinPrice != nil {
		q.Set("minPrice", fmt.Sprintf("%d", *params.MinPrice))
	}
	if params.MaxPrice != nil {
		q.Set("maxPrice", fmt.Sprintf("%d", *params.MaxPrice))
	}
	if params.ModAfter != "" {
		q.Set("modAfter", params.ModAfter)
	}
	return base + "?" + q.Encode()
}

func BuildAdURL(adID string) string {
	return fmt.Sprintf("https://api.kleinanzeigen.de/api/ads/%s.json", adID)
}

func BuildViewsURL(adID string) string {
	return fmt.Sprintf("https://www.kleinanzeigen.de/s-vac-inc-get.json?adId=%s", adID)
}

func ParseViewsResponse(body []byte) int {
	var resp struct {
		Counter struct {
			NumVisits int `json:"numVisits"`
		} `json:"counter"`
	}
	if json.Unmarshal(body, &resp) == nil {
		return resp.Counter.NumVisits
	}
	var simple map[string]interface{}
	if json.Unmarshal(body, &simple) == nil {
		if c, ok := simple["counter"].(map[string]interface{}); ok {
			if v, ok := c["numVisits"].(float64); ok {
				return int(v)
			}
		}
	}
	return 0
}

func BuildBatchViewsURL(adIDs []string) string {
	return fmt.Sprintf("https://api.kleinanzeigen.de/api/v2/counters/ads/vip?adIds=%s", strings.Join(adIDs, ","))
}

func BuildBatchFavoritesURL(adIDs []string) string {
	return fmt.Sprintf("https://api.kleinanzeigen.de/api/v2/counters/ads/watchlist?adIds=%s", strings.Join(adIDs, ","))
}

type CounterEntry struct {
	AdID  string `json:"adId"`
	Value int    `json:"value"`
}

func ParseCountersResponse(body []byte) map[string]int {
	var resp struct {
		Counters []CounterEntry `json:"counters"`
	}
	result := make(map[string]int)
	if json.Unmarshal(body, &resp) == nil {
		for _, c := range resp.Counters {
			result[c.AdID] = c.Value
		}
	}
	return result
}

func AdPublicURL(adID string) string {
	return fmt.Sprintf("https://www.kleinanzeigen.de/s-anzeige/%s", adID)
}

func GenerateIOSUserAgent(version string) string {
	iosMajor := 16
	iosMinor := rand.Intn(6)
	iosPatch := rand.Intn(10)

	buildMajor := 23 + rand.Intn(4)
	buildMinor := 100 + rand.Intn(900)
	buildPatch := 11000000 + rand.Intn(2000000)

	alamoMinor := 8 + rand.Intn(3)
	alamoPatch := rand.Intn(4)

	return fmt.Sprintf(
		"Kleinanzeigen/%s (com.ebaykleinanzeigen.ebc; build:%d.%d.%d; iOS %d.%d.%d) Alamofire/5.%d.%d",
		version, buildMajor, buildMinor, buildPatch,
		iosMajor, iosMinor, iosPatch, alamoMinor, alamoPatch,
	)
}

func GenerateSessionID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func GenerateAPIHeaders(version, basicAuth string) http.Header {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Accept-Encoding", "gzip")
	h.Set("X-Ebayk-Wenkse-Session-Id", GenerateSessionID())
	h.Set("X-EBAYK-APP", "2B120C85-DDC3-4236-BEA7-391DFB533A1B")
	h.Set("X-EBAYK-USERID-TOKEN", "")
	h.Set("X-EBAYK-GROUPS", "BAND-7832-Category-Alerts_B|BAND-8364_A|BLN-19260-cis-login_B")
	h.Set("Authorization", basicAuth)
	h.Set("User-Agent", GenerateIOSUserAgent(version))
	h.Set("X-ECG-USER-AGENT", "ebayk-iphone-app-2518511418194")
	h.Set("X-ECG-USER-VERSION", version)
	return h
}

func ParseAdResponse(body []byte) (*Ad, []string, error) {
	var resp map[string]interface{}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, nil, fmt.Errorf("json unmarshal: %w", err)
	}

	adKey := "{http://www.ebayclassifiedsgroup.com/schema/ad/v1}ad"
	adWrapper, ok := resp[adKey]
	if !ok {
		return nil, nil, fmt.Errorf("missing ad key in response")
	}

	adMap, ok := adWrapper.(map[string]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("invalid ad wrapper type")
	}

	value, ok := adMap["value"].(map[string]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("missing value in ad")
	}

	now := time.Now()
	ad := &Ad{
		IsActive:    true,
		FirstSeenAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if v := getNestedString(value, "id"); v != "" {
		ad.ID = v
	}
	if v := getNestedString(value, "title", "value"); v != "" {
		ad.Title = v
	}
	if v := getNestedString(value, "description", "value"); v != "" {
		ad.Description = strings.ReplaceAll(v, "<br />", "\n")
	}
	if v := getNestedString(value, "price", "amount", "value"); v != "" {
		var f float64
		fmt.Sscanf(v, "%f", &f)
		ad.Price = int64(f)
		ad.PriceEUR = f
	}
	if v := getNestedString(value, "contact-name", "value"); v != "" {
		ad.ContactName = v
	}
	if v := getNestedString(value, "category", "id"); v != "" {
		ad.CategoryID = v
	}
	if v := getNestedString(value, "ad-status", "value"); v != "" {
		ad.AdStatus = v
	}
	if v := getNestedString(value, "user-id", "value"); v != "" {
		ad.UserID = v
	}
	if v := getNestedString(value, "user-since-date-time", "value"); v != "" {
		ad.UserSinceDate = v
	}
	if v := getNestedString(value, "poster-type", "value"); v != "" {
		ad.PosterType = v
	}
	if v := getNestedString(value, "start-date-time", "value"); v != "" {
		ad.StartDate = v
	}
	if v := getNestedString(value, "last-user-edit-date", "value"); v != "" {
		ad.LastEditDate = v
	}
	if v := getNestedString(value, "seller-account-type", "value"); v != "" {
		ad.SellerAccountType = v
	}
	if buyNow, ok := value["buy-now"].(map[string]interface{}); ok {
		if enabled, ok := buyNow["buy-now-enabled"].(string); ok && enabled == "true" {
			ad.BuyNowEnabled = true
		}
	}

	if locations, ok := value["locations"].(map[string]interface{}); ok {
		if locList, ok := locations["location"].([]interface{}); ok && len(locList) > 0 {
			if loc, ok := locList[0].(map[string]interface{}); ok {
				if v, ok := loc["id"].(string); ok {
					ad.LocationID = v
				}
			}
		}
	}

	if shipping, ok := value["shipping-options"].(map[string]interface{}); ok {
		if opts, ok := shipping["shipping-option"].([]interface{}); ok && len(opts) > 0 {
			if opt, ok := opts[0].(map[string]interface{}); ok {
				if v, ok := opt["id"].(string); ok {
					ad.ShippingOption = v
				}
			}
		}
	}

	ad.URL = AdPublicURL(ad.ID)

	var photos []string
	if pics, ok := value["pictures"].(map[string]interface{}); ok {
		if picList, ok := pics["picture"].([]interface{}); ok {
			for _, p := range picList {
				if pic, ok := p.(map[string]interface{}); ok {
					bestURL := extractBestImageURL(pic)
					if bestURL != "" {
						photos = append(photos, bestURL)
					}
				}
			}
		}
	}

	return ad, photos, nil
}

func ParseSearchResponse(body []byte) (*SearchResult, error) {
	var resp map[string]interface{}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	adsKey := "{http://www.ebayclassifiedsgroup.com/schema/ad/v1}ads"
	adsWrapper, ok := resp[adsKey]
	if !ok {
		return nil, fmt.Errorf("missing ads key")
	}

	adsOuter, ok := adsWrapper.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid ads wrapper")
	}

	adsMap := adsOuter
	if inner, ok := adsOuter["value"].(map[string]interface{}); ok {
		adsMap = inner
	}

	result := &SearchResult{}

	if paging, ok := adsMap["paging"].(map[string]interface{}); ok {
		result.Paging.NumFound, _ = paging["numFound"].(string)
		if links, ok := paging["link"].([]interface{}); ok {
			for _, l := range links {
				if lm, ok := l.(map[string]interface{}); ok {
					if rel, _ := lm["rel"].(string); rel == "next" {
						result.Paging.Next, _ = lm["href"].(string)
					}
				}
			}
		}
	}

	result.Raw = adsMap

	adList, ok := adsMap["ad"].([]interface{})
	if !ok {
		return result, nil
	}

	for _, a := range adList {
		aMap, ok := a.(map[string]interface{})
		if !ok {
			continue
		}
		raw := RawAd{Raw: aMap}
		if id, ok := aMap["id"].(string); ok {
			raw.ID = id
		}
		result.Ads = append(result.Ads, raw)
	}

	return result, nil
}

func ParseAdFromSearchResult(raw map[string]interface{}) (*Ad, []string) {
	now := time.Now()
	ad := &Ad{
		IsActive:    true,
		FirstSeenAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if v, ok := raw["id"].(string); ok {
		ad.ID = v
	}
	if t, ok := raw["title"].(map[string]interface{}); ok {
		if v, ok := t["value"].(string); ok {
			ad.Title = v
		}
	}
	if d, ok := raw["description"].(map[string]interface{}); ok {
		if v, ok := d["value"].(string); ok {
			ad.Description = strings.ReplaceAll(v, "<br />", "\n")
		}
	}
	if p, ok := raw["price"].(map[string]interface{}); ok {
		if amt, ok := p["amount"].(map[string]interface{}); ok {
			if v, ok := amt["value"].(float64); ok {
				ad.Price = int64(v)
				ad.PriceEUR = v
			}
		}
	}
	if v := getNestedString(raw, "ad-status", "value"); v != "" {
		ad.AdStatus = v
	}
	if v := getNestedString(raw, "poster-type", "value"); v != "" {
		ad.PosterType = v
	}
	if v := getNestedString(raw, "start-date-time", "value"); v != "" {
		ad.StartDate = v
	}
	if v := getNestedString(raw, "contact-name", "value"); v != "" {
		ad.ContactName = v
	}
	if v := getNestedString(raw, "user-id", "value"); v != "" {
		ad.UserID = v
	}
	if v := getNestedString(raw, "user-since-date-time", "value"); v != "" {
		ad.UserSinceDate = v
	}
	if cat, ok := raw["category"].(map[string]interface{}); ok {
		if v, ok := cat["id"].(string); ok {
			ad.CategoryID = v
		}
	}
	if locs, ok := raw["locations"].(map[string]interface{}); ok {
		if locList, ok := locs["location"].([]interface{}); ok && len(locList) > 0 {
			if loc, ok := locList[0].(map[string]interface{}); ok {
				if v, ok := loc["id"].(string); ok {
					ad.LocationID = v
				}
			}
		}
	}
	if shipping, ok := raw["shipping-options"].(map[string]interface{}); ok {
		if opts, ok := shipping["shipping-option"].([]interface{}); ok && len(opts) > 0 {
			if opt, ok := opts[0].(map[string]interface{}); ok {
				if v, ok := opt["id"].(string); ok {
					ad.ShippingOption = v
				}
			}
		}
	}
	if v := getNestedString(raw, "seller-account-type", "value"); v != "" {
		ad.SellerAccountType = v
	}
	if v := getNestedString(raw, "last-user-edit-date", "value"); v != "" {
		ad.LastEditDate = v
	}
	if buyNow, ok := raw["buy-now"].(map[string]interface{}); ok {
		if sel, ok := buyNow["selected"].(string); ok && sel == "true" {
			ad.BuyNowEnabled = true
		}
	}

	ad.URL = AdPublicURL(ad.ID)

	var photos []string
	if pics, ok := raw["pictures"].(map[string]interface{}); ok {
		if picList, ok := pics["picture"].([]interface{}); ok {
			for _, p := range picList {
				if pic, ok := p.(map[string]interface{}); ok {
					if u := extractBestImageURL(pic); u != "" {
						photos = append(photos, u)
					}
				}
			}
		}
	}

	return ad, photos
}

func ImageHash(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}

func extractBestImageURL(pic map[string]interface{}) string {
	links, ok := pic["link"].([]interface{})
	if !ok {
		return ""
	}

	priority := map[string]int{
		"extraLarge":   1,
		"large":        2,
		"XXL":          3,
		"canonicalUrl": 4,
	}

	bestURL := ""
	bestPri := 999

	for _, l := range links {
		link, ok := l.(map[string]interface{})
		if !ok {
			continue
		}
		rel, _ := link["rel"].(string)
		href, _ := link["href"].(string)
		if href == "" {
			continue
		}
		if p, ok := priority[rel]; ok && p < bestPri {
			bestPri = p
			bestURL = href
		} else if bestURL == "" {
			bestURL = href
		}
	}
	return bestURL
}

func ParseCategoriesResponse(body []byte) ([]CategoryNode, error) {
	var resp map[string]interface{}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	catKey := "{http://www.ebayclassifiedsgroup.com/schema/category/v1}categories"
	wrapper, ok := resp[catKey]
	if !ok {
		return nil, fmt.Errorf("missing categories key")
	}
	wrapperMap, ok := wrapper.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid categories wrapper")
	}
	value, ok := wrapperMap["value"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("missing value")
	}

	var all []CategoryNode
	catList, ok := value["category"].([]interface{})
	if !ok {
		return nil, nil
	}

	var flatten func(cats []interface{}, parentID string, level int)
	flatten = func(cats []interface{}, parentID string, level int) {
		for _, c := range cats {
			cMap, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			node := CategoryNode{
				ParentID: parentID,
				Level:    level,
			}
			if id, ok := cMap["id"].(string); ok {
				node.ID = id
			}
			if name, ok := cMap["localized-name"].(map[string]interface{}); ok {
				if v, ok := name["value"].(string); ok {
					node.Name = v
				}
			}
			if children, ok := cMap["category"].([]interface{}); ok && len(children) > 0 {
				node.HasChildren = true
				flatten(children, node.ID, level+1)
			}
			all = append(all, node)
		}
	}

	flatten(catList, "", 0)
	return all, nil
}

func getNestedString(m map[string]interface{}, keys ...string) string {
	current := m
	for i, key := range keys {
		if i == len(keys)-1 {
			if v, ok := current[key]; ok {
				switch val := v.(type) {
				case string:
					return val
				case float64:
					return fmt.Sprintf("%g", val)
				}
			}
			return ""
		}
		next, ok := current[key].(map[string]interface{})
		if !ok {
			return ""
		}
		current = next
	}
	return ""
}
