# JSONata Functions

The following built-in JSONata utility functions are available for use in any rule with a Rule Type of JSONata. Each function is invoked by including `$utils.` followed by the function name and any arguments.

## `parse_refs($from, $to, $within)`

### Purpose

This function returns a copy of the object or array specified in `$within`, within which all occurrences of the `$from` attribute have been recursively replaced with the `$to` attribute, where:

- The `$from` attribute contains one or more id reference values,
- The `$to` attribute will contain the object(s) corresponding with the id values referenced in the `$from` attribute.
- The `$within` argument defines the scope within which:
  - Objects will have the `$from` attribute replaced with the `$to` attribute.
  - The objects referenced by the id in the `$from` attribute can be found.

### Arguments:

- `$from`: A string value indicating the name of the attribute containing id reference values. For example, "nextId" or "childIds".
- `$to`: A string value specifying the name of the attribute that will contain replacement objects. For example, "next" or "children".
- `$within`: An object or array of objects that contains the sub-objects that:

  - Contain the `$from` attribute to be replaced
  - Are referenced by the id values in any `$from` argument.

  For example, `study.versions[0].studyDesigns[0].encounters`

  The size of this object will affect the processing speed for the function - the smaller the object, the faster the processing.

### Example

#### Input

```json
(
    $ordered_objects :=
        [
            {
                "id": "Object_1",
                "name": "OBJ1",
                "nextId": "Object_2"
            },
            {
                "id": "Object_2",
                "name": "OBJ2",
                "nextId": "Object_3"
            },
            {
                "id": "Object_3",
                "name": "OBJ3",
                "nextId": null
            }
        ];

    $parsed := $utils.parse_refs("nextId","next",$ordered_objects);
    $parsed
)
```

#### Result

```json
[
  {
    "id": "Object_1",
    "name": "OBJ1",
    "next": {
      "id": "Object_2",
      "name": "OBJ2",
      "next": {
        "id": "Object_3",
        "name": "OBJ1"
      }
    }
  },
  {
    "id": "Object_2",
    "name": "OBJ2",
    "next": {
      "id": "Object_3",
      "name": "OBJ1"
    }
  },
  {
    "id": "Object_3",
    "name": "OBJ1"
  }
]
```

**Note**: If the `$from` attribute contains:

- An array of valid id reference values, the corresponding replacement `$to` attribute will contain an array of objects.
- No valid id values (i.e., no value that matches the value of the id attribute of any sub-object of `$within`), the `$from` attribute will be removed, but the `$to` attribute will not be added (as shown above).
- The id value of the current object, or of any other object that references the current object either directly or indirectly, a `circularReference` indicator will be included in the `$to` attribute instead of the referenced object.

## `sift_tree($tree, $find_val, $include, $prefix)`

### Purpose

This function returns a copy of the input `$tree` object that has been "trimmed" to show only the sub-branch(es) recursively containing the specified `$find_val` object, where each object in the sub-branch(es) contains only the attribute that (recursively) contains `$find_val` and any attribute(s) specified in `$include`. If `$prefix` is `true`, each attribute name in the sub-branch(es) will be prefixed with the value of the object's `instanceType` attribute.

### Arguments

- `$tree`: An object containing `$find_val` at some level.
- `$find_val`: Any value (primitive, object or array) to be found within `$tree`. This will usually be a unique object.
- `$include`: A list of attribute names to be displayed in sub-branch objects in addition to the attribute that contains `$find_val`. Specify an empty list (`[]`) if no additional attributes are needed.
- `$prefix`: [Optional] A boolean value to indicate whether sub-branch attribute names should (`true`) or should not (`false`) be prefixed with the value of the `instanceType` attribute. Default is `false`.

### Example

#### Input

```json
(
    $example_study :=
        {
            "study": {
                "id": "Study_1",
                "name": "CDISC PILOT - LZZT",
                "description": null,
                "label": null,
                "versions": [
                {
                    "id": "StudyVersion_1",
                    "versionIdentifier": "2",
                    "rationale": "The discontinuation rate associated with this oral dosing regimen was 58.6% in previous studies, and alternative clinical strategies have been sought to improve tolerance for the compound. To that end, development of a Transdermal Therapeutic System (TTS) has been initiated.",
                    "studyDesigns": [
                    {
                        "id": "InterventionalStudyDesign_1",
                        "name": "Study Design 1",
                        "label": "USDM Example Study Design",
                        "description": "The main design for the study",
                        "intentTypes": [
                        {
                            "id": "Code_152",
                            "code": "C49656",
                            "codeSystem": "http://www.cdisc.org",
                            "codeSystemVersion": "2025-09-26",
                            "decode": "Treatment Study",
                            "instanceType": "Code"
                        },
                        {
                            "id": "Code_152_a",
                            "code": "C49654",
                            "codeSystem": "http://www.cdisc.org",
                            "codeSystemVersion": "2025-09-26",
                            "decode": "Cure Study",
                            "instanceType": "Code"
                        }
                        ],
                        "instanceType": "InterventionalStudyDesign"
                    }
                    ],
                    "notes": [],
                    "instanceType": "StudyVersion"
                }
                ],
                "instanceType": "Study"
            }
        };
    $value_to_find_1 := "Cure Study";
    $value_to_find_2 := "http://www.cdisc.org";
    $value_to_find_3 := $example_study.study.versions[0].studyDesigns[0].intentTypes[1];
)
```

#### Results for `$utils.sift_tree($example_study,$value_to_find_1,[])`

```json
{
  "study": {
    "versions": [
      {
        "studyDesigns": [
          {
            "intentTypes": [
              {
                "decode": "Cure Study"
              }
            ]
          }
        ]
      }
    ]
  }
}
```

#### Results for `$utils.sift_tree($example_study,$value_to_find_2,["id","code"])`

```json
{
  "study": {
    "id": "Study_1",
    "versions": [
      {
        "id": "StudyVersion_1",
        "studyDesigns": [
          {
            "id": "InterventionalStudyDesign_1",
            "intentTypes": [
              {
                "id": "Code_152",
                "code": "C49656",
                "codeSystem": "http://www.cdisc.org"
              },
              {
                "id": "Code_152_a",
                "code": "C49654",
                "codeSystem": "http://www.cdisc.org"
              }
            ]
          }
        ]
      }
    ]
  }
}
```

#### Results for `$utils.sift_tree($example_study,$value_to_find_3,["id","name"],true)`

```json
{
  "study": {
    "Study.id": "Study_1",
    "Study.name": "CDISC PILOT - LZZT",
    "Study.versions": [
      {
        "StudyVersion.id": "StudyVersion_1",
        "StudyVersion.studyDesigns": [
          {
            "InterventionalStudyDesign.id": "InterventionalStudyDesign_1",
            "InterventionalStudyDesign.name": "Study Design 1",
            "InterventionalStudyDesign.intentTypes": [
              {
                "Code.id": "Code_152_a"
              }
            ]
          }
        ]
      }
    ]
  }
}
```
