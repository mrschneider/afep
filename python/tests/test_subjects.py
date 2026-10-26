import unittest

from afep import subjects


class SubjectTests(unittest.TestCase):
    def test_roundtrip(self):
        s = subjects.subject("edge", "node7", subjects.VERB_ENVELOPES)
        self.assertEqual(s, "afep.edge.node7.envelopes")
        self.assertEqual(subjects.parse_subject(s), ("edge", "node7", "envelopes"))

    def test_validation(self):
        with self.assertRaises(ValueError):
            subjects.subject("", "x", subjects.VERB_ENVELOPES)
        with self.assertRaises(ValueError):
            subjects.subject("edge", "a b", subjects.VERB_ENVELOPES)
        with self.assertRaises(ValueError):
            subjects.subject("edge", "n.1", subjects.VERB_ENVELOPES)
        with self.assertRaises(ValueError):
            subjects.subject("edge", "n1", "")

    def test_wildcard_allowed(self):
        s = subjects.subject("edge", subjects.WILDCARD, subjects.VERB_ENVELOPES)
        self.assertEqual(s, "afep.edge.*.envelopes")

    def test_parse_rejects_garbage(self):
        for bad in ("foo.bar", "afep.a.b", "afep.a.b.c.d", ""):
            with self.assertRaises(ValueError):
                subjects.parse_subject(bad)


if __name__ == "__main__":
    unittest.main()
