'use strict';

var _ = require('lodash');
var Form = require('./Form');
var Promise = require('bluebird');
var Response = require('./Response');
var Set = require('collections/set');

Form.base = require('mongoose');
Promise.promisifyAll(Form);

function getLatestForm(surveyId) {
  return Promise.resolve(Form.find({
    survey: surveyId
  }).sort({
    created: 'desc'
  }).limit(1).lean().exec());
}

// Return a promise for a function that sorts fields according to their order in
// the survey form.
exports.getFieldSorter = function getFieldSorter(surveyId) {
  // Set maintains insertion order and tracks uniqueness
  var fields = new Set();

  // Adds the field name if it does not exist. Otherwise, adds a unique,
  // suffixed version of the field name.
  function addFieldUniquely(name) {
    var test = name;
    var i = 1;
    while (!fields.add(test)) {
      i += 1;
      test = name + '-' + i;
    }
    return test;
  }

  // Traverse the form depth-first and record unique field names in the order we
  // encounter them.
  function processQuestions(questions) {
    _.forEach(questions, function (question) {
      if (question.type === 'checkbox') {
        _.forEach(question.answers, function (answer) {
          addFieldUniquely(answer.name);
          if (answer.questions) {
            processQuestions(answer.questions);
          }
        });
      } else {
        addFieldUniquely(question.name);
        if (question.answers) {
          _.forEach(question.answers, function (answer) {
            if (answer.questions) {
              processQuestions(answer.questions);
            }
          });
        }
      }
    });
  }

  return getLatestForm(surveyId).then(function (docs) {
    if (docs.length === 0) {
      console.log("No forms found for survey " + surveyId);
      return; // XXX Todo: handle this case
    }

    processQuestions(docs[0].questions);
    var fieldOrder = {};
    fields.enumerate().forEach(function (pair) {
      fieldOrder[pair[1]] = pair[0];
    });

    return function sort(columns) {
      return _.sortBy(columns, function (field) {
        var index = fieldOrder[field];
        if (index === undefined) {
          index = Number.POSITIVE_INFINITY;
        }
        return index;
      });
    };
  });
};
