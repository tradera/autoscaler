/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package annotations

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestExternalOOMAnnotationRoundTripAndBounds(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	var signals []ExternalOOMSignal
	for i := range 200 {
		signals = append(signals, ExternalOOMSignal{Metric: strings.Repeat("m", 1024), PodName: "worker", PodUID: types.UID(fmt.Sprint(i)), ContainerName: "app", ObservedAt: metav1.NewTime(now.Add(-time.Duration(i) * time.Second)), MemoryRequest: resource.MustParse("1Gi")})
	}
	existing := map[string]string{"other": "keep"}
	raw, err := EncodeExternalOOMSignals(signals, existing)
	require.NoError(t, err)
	require.LessOrEqual(t, len(raw), MaxExternalOOMSignalBytes)
	decoded := ParseExternalOOMSignals(map[string]string{ExternalOOMSignalsAnnotation: raw})
	require.NotEmpty(t, decoded)
	require.Less(t, len(decoded), MaxExternalOOMSignals, "byte budget applies before count cap")
	require.Equal(t, types.UID("0"), decoded[0].PodUID)
	require.Equal(t, "1Gi", decoded[0].MemoryRequest.String())
	require.WithinDuration(t, now, decoded[0].ObservedAt.Time, time.Nanosecond)
	require.Equal(t, map[string]string{"other": "keep"}, existing, "encoding must not mutate configuration")
	require.Len(t, signals, 200)
	// Exhausting the remaining Kubernetes budget must not create an invalid VPA.
	raw, err = EncodeExternalOOMSignals(signals, map[string]string{"other": strings.Repeat("x", 256*1024-len("other"))})
	require.NoError(t, err)
	require.Empty(t, raw)
	// Count limit is independent of the transport-byte budget.
	small := signals[:]
	for i := range small {
		small[i].Metric = "oom"
	}
	raw, err = EncodeExternalOOMSignals(small, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, len(ParseExternalOOMSignals(map[string]string{ExternalOOMSignalsAnnotation: raw})), MaxExternalOOMSignals)
}

func TestExternalOOMAnnotationMalformedDataFailsClosed(t *testing.T) {
	for _, raw := range []string{"", "null", "garbage", `{"version":2,"signals":[]}`, strings.Repeat("x", MaxExternalOOMSignalBytes+1), `{"version":1,"signals":[` + strings.Repeat("{},", 128) + `{}]}`, `{"version":1,"signals":[{"memoryRequest":"invalid"}]}`} {
		require.Empty(t, ParseExternalOOMSignals(map[string]string{ExternalOOMSignalsAnnotation: raw}))
	}
}
