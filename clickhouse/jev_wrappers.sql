-- Public functions on top of the executable UDF jev_eval(row, question, kind, options).
-- `row` is a String: formatRowNoNewline('JSONEachRow', col1, col2) for a JSON object of named columns,
-- or any text column on its own.

-- Probability (0..1) that the row satisfies the plain-language condition.
CREATE OR REPLACE FUNCTION jev_prob AS (row, condition) ->
    JSONExtractFloat(jev_eval(row, condition, 'noul', emptyArrayString()), 'noul');

-- Boolean predicate at the 0.5 threshold. For another threshold write jev_prob(...) >= 0.7.
CREATE OR REPLACE FUNCTION jev AS (row, condition) -> jev_prob(row, condition) >= 0.5;

-- Graded rating along ordered levels: the probability-weighted level index (0 .. n-1).
CREATE OR REPLACE FUNCTION jev_score AS (row, question, levels) ->
    JSONExtractFloat(jev_eval(row, question, 'score', levels), 'score');

-- Same, normalised to 0..1 so it is comparable across rubrics.
CREATE OR REPLACE FUNCTION jev_score_norm AS (row, question, levels) ->
    jev_score(row, question, levels) / greatest(length(levels) - 1, 1);

-- The most likely option for the row.
CREATE OR REPLACE FUNCTION jev_choice AS (row, question, options) ->
    JSONExtractString(jev_eval(row, question, 'choice', options), 'choice');

-- Confidence (0..1) of a 'choice' or 'score' answer.
CREATE OR REPLACE FUNCTION jev_confidence AS (row, question, kind, options) ->
    JSONExtractFloat(jev_eval(row, question, kind, options), 'confidence');
