package parser

import (
	"strings"
	"time"

	kl "github.com/danamakarenko/klaz-parser/pkg/kleinanzeigen"
)

type Filter struct{}

func NewFilter() *Filter {
	return &Filter{}
}

func (f *Filter) Apply(ad *kl.Ad, filters *kl.ParseFilters) bool {
	if filters == nil {
		return true
	}

	if filters.DateToday {
		now := time.Now()
		todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		if adDate := parseDate(ad.StartDate); !adDate.IsZero() && adDate.Before(todayStart) {
			return false
		}
	}

	if filters.Last24Hours {
		cutoff := time.Now().Add(-24 * time.Hour)
		if adDate := parseDate(ad.StartDate); !adDate.IsZero() && adDate.Before(cutoff) {
			return false
		}
	}

	if filters.DateFrom != "" {
		if from := parseDate(filters.DateFrom); !from.IsZero() {
			if adDate := parseDate(ad.StartDate); !adDate.IsZero() && adDate.Before(from) {
				return false
			}
		}
	}

	if filters.DateTo != "" {
		if to := parseDate(filters.DateTo); !to.IsZero() {
			if adDate := parseDate(ad.StartDate); !adDate.IsZero() && adDate.After(to) {
				return false
			}
		}
	}

	if filters.PriceMin != nil && ad.PriceEUR < *filters.PriceMin {
		return false
	}
	if filters.PriceMax != nil && ad.PriceEUR > *filters.PriceMax {
		return false
	}
	if ad.PriceEUR > 50000 {
		return false
	}

	if filters.ViewsMin != nil && ad.Views < *filters.ViewsMin {
		return false
	}
	if filters.ViewsMax != nil && ad.Views > *filters.ViewsMax {
		return false
	}

	if filters.SellerAccountStatus != "" && ad.PosterType != filters.SellerAccountStatus {
		return false
	}

	if filters.SellerRegYearMin != nil || filters.SellerRegYearMax != nil {
		if regDate := parseDate(ad.UserSinceDate); !regDate.IsZero() {
			year := regDate.Year()
			if filters.SellerRegYearMin != nil && year < *filters.SellerRegYearMin {
				return false
			}
			if filters.SellerRegYearMax != nil && year > *filters.SellerRegYearMax {
				return false
			}
		}
	}

	if len(filters.DescriptionSkipPhrases) > 0 {
		descLower := strings.ToLower(ad.Description)
		for _, phrase := range filters.DescriptionSkipPhrases {
			if strings.Contains(descLower, strings.ToLower(phrase)) {
				return false
			}
		}
	}

	if filters.SicherBezahlt {
		hasSecure := strings.Contains(strings.ToLower(ad.ShippingOption), "sicher") ||
			strings.Contains(strings.ToLower(ad.Description), "sicher bezahlt")
		if !hasSecure {
			return false
		}
	}

	return true
}

func parseDate(s string) time.Time {
	if s == "" {
		return time.Time{}
	}

	formats := []string{
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000+0200",
		"2006-01-02T15:04:05.000-0700",
		"2006-01-02",
	}

	clean := s
	if idx := strings.Index(clean, "+"); idx > 10 {
		clean = clean[:idx]
	}
	if idx := strings.Index(clean, "."); idx > 10 {
		clean = clean[:idx]
	}

	for _, format := range formats {
		if t, err := time.Parse(format, clean); err == nil {
			return t
		}
	}
	return time.Time{}
}
