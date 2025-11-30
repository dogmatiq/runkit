package eventstream

// func TestComputeCollision(t *testing.T) {
// 	cases := []struct {
// 		Name          string
// 		WantIdentical bool
// 		WantCollision bool
// 		LHS           []*envelopepb.Envelope
// 		RHS           []*envelopepb.Envelope
// 	}{
// 		{
// 			"both empty",
// 			true,
// 			false,
// 			nil,
// 			nil,
// 		},
// 		{
// 			"one empty",
// 			false,
// 			false,
// 			nil,
// 			[]*envelopepb.Envelope{
// 				{MessageId: uuidpb.MustParse("956a9a6b-81ea-43c7-be61-4906a3495d5a")},
// 			},
// 		},
// 		{
// 			"identical",
// 			true,
// 			true,
// 			[]*envelopepb.Envelope{
// 				{MessageId: uuidpb.MustParse("956a9a6b-81ea-43c7-be61-4906a3495d5a")},
// 				{MessageId: uuidpb.MustParse("35d260ff-4996-400f-9fc8-03d10d706ab9")},
// 				{MessageId: uuidpb.MustParse("db0cf11a-5a7c-42ba-970f-615e0dbfbc7d")},
// 			},
// 			[]*envelopepb.Envelope{
// 				{MessageId: uuidpb.MustParse("956a9a6b-81ea-43c7-be61-4906a3495d5a")},
// 				{MessageId: uuidpb.MustParse("35d260ff-4996-400f-9fc8-03d10d706ab9")},
// 				{MessageId: uuidpb.MustParse("db0cf11a-5a7c-42ba-970f-615e0dbfbc7d")},
// 			},
// 		},
// 		{
// 			"equivalent but reordered",
// 			false,
// 			true,
// 			[]*envelopepb.Envelope{
// 				{MessageId: uuidpb.MustParse("956a9a6b-81ea-43c7-be61-4906a3495d5a")},
// 				{MessageId: uuidpb.MustParse("35d260ff-4996-400f-9fc8-03d10d706ab9")},
// 				{MessageId: uuidpb.MustParse("db0cf11a-5a7c-42ba-970f-615e0dbfbc7d")},
// 			},
// 			[]*envelopepb.Envelope{
// 				{MessageId: uuidpb.MustParse("35d260ff-4996-400f-9fc8-03d10d706ab9")},
// 				{MessageId: uuidpb.MustParse("956a9a6b-81ea-43c7-be61-4906a3495d5a")},
// 				{MessageId: uuidpb.MustParse("db0cf11a-5a7c-42ba-970f-615e0dbfbc7d")},
// 			},
// 		},
// 		{
// 			"equivalent but reordered",
// 			false,
// 			true,
// 			[]*envelopepb.Envelope{
// 				{MessageId: uuidpb.MustParse("956a9a6b-81ea-43c7-be61-4906a3495d5a")},
// 				{MessageId: uuidpb.MustParse("35d260ff-4996-400f-9fc8-03d10d706ab9")},
// 				{MessageId: uuidpb.MustParse("db0cf11a-5a7c-42ba-970f-615e0dbfbc7d")},
// 			},
// 			[]*envelopepb.Envelope{
// 				{MessageId: uuidpb.MustParse("35d260ff-4996-400f-9fc8-03d10d706ab9")},
// 				{MessageId: uuidpb.MustParse("956a9a6b-81ea-43c7-be61-4906a3495d5a")},
// 				{MessageId: uuidpb.MustParse("db0cf11a-5a7c-42ba-970f-615e0dbfbc7d")},
// 			},
// 		},
// 		{
// 			"prefix match",
// 			false,
// 			true,
// 			[]*envelopepb.Envelope{
// 				{MessageId: uuidpb.MustParse("956a9a6b-81ea-43c7-be61-4906a3495d5a")},
// 				{MessageId: uuidpb.MustParse("35d260ff-4996-400f-9fc8-03d10d706ab9")},
// 				{MessageId: uuidpb.MustParse("db0cf11a-5a7c-42ba-970f-615e0dbfbc7d")},
// 			},
// 			[]*envelopepb.Envelope{
// 				{MessageId: uuidpb.MustParse("956a9a6b-81ea-43c7-be61-4906a3495d5a")},
// 				{MessageId: uuidpb.MustParse("35d260ff-4996-400f-9fc8-03d10d706ab9")},
// 			},
// 		},
// 		{
// 			"suffix match",
// 			false,
// 			true,
// 			[]*envelopepb.Envelope{
// 				{MessageId: uuidpb.MustParse("956a9a6b-81ea-43c7-be61-4906a3495d5a")},
// 				{MessageId: uuidpb.MustParse("35d260ff-4996-400f-9fc8-03d10d706ab9")},
// 				{MessageId: uuidpb.MustParse("db0cf11a-5a7c-42ba-970f-615e0dbfbc7d")},
// 			},
// 			[]*envelopepb.Envelope{
// 				{MessageId: uuidpb.MustParse("35d260ff-4996-400f-9fc8-03d10d706ab9")},
// 				{MessageId: uuidpb.MustParse("db0cf11a-5a7c-42ba-970f-615e0dbfbc7d")},
// 			},
// 		},
// 		{
// 			"infix match",
// 			false,
// 			true,
// 			[]*envelopepb.Envelope{
// 				{MessageId: uuidpb.MustParse("956a9a6b-81ea-43c7-be61-4906a3495d5a")},
// 				{MessageId: uuidpb.MustParse("35d260ff-4996-400f-9fc8-03d10d706ab9")},
// 				{MessageId: uuidpb.MustParse("db0cf11a-5a7c-42ba-970f-615e0dbfbc7d")},
// 			},
// 			[]*envelopepb.Envelope{
// 				{MessageId: uuidpb.MustParse("35d260ff-4996-400f-9fc8-03d10d706ab9")},
// 			},
// 		},
// 	}

// 	for _, c := range cases {
// 		t.Run(c.Name, func(t *testing.T) {
// 			gotIdentical, gotCollision := hasCollision(c.LHS, c.RHS)

// 			if gotIdentical != c.WantIdentical {
// 				t.Fatalf("unexpected identical: got %t, want %t", gotIdentical, c.WantIdentical)
// 			}

// 			if gotCollision != c.WantCollision {
// 				t.Fatalf("unexpected collision: got %t, want %t", gotCollision, c.WantCollision)
// 			}
// 		})

// 		t.Run(c.Name+" (reversed)", func(t *testing.T) {
// 			gotIdentical, gotCollision := hasCollision(c.RHS, c.LHS)

// 			if gotIdentical != c.WantIdentical {
// 				t.Fatalf("unexpected identical: got %t, want %t", gotIdentical, c.WantIdentical)
// 			}

// 			if gotCollision != c.WantCollision {
// 				t.Fatalf("unexpected collision: got %t, want %t", gotCollision, c.WantCollision)
// 			}
// 		})

// 		t.Run(c.Name+" (does not allocate)", func(t *testing.T) {
// 			allocs := testing.AllocsPerRun(100, func() {
// 				hasCollision(c.LHS, c.RHS)
// 			})

// 			if allocs != 0 {
// 				t.Fatalf("expected zero allocations, got %f", allocs)
// 			}
// 		})
// 	}
// }
