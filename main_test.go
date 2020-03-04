package main

import "testing"

func Test_geoLocation_distanceTo(t *testing.T) {
	type fields struct {
		lat  float64
		long float64
	}
	type args struct {
		loc geoLocation
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   distance
	}{
		{
			name: "London To Kaunas",
			fields: fields{
				lat:  51.533081,
				long: -0.109664,
			},
			args: args{
				loc: geoLocation{
					lat:  54.901332,
					long: 23.894786,
				},
			},
			want: 1632,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := geoLocation{
				lat:  tt.fields.lat,
				long: tt.fields.long,
			}
			if got := g.distanceTo(tt.args.loc); got != tt.want {
				t.Errorf("distanceTo() = %v, want %v", got, tt.want)
			}
		})
	}
}
