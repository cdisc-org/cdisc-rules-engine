from cdisc_rules_engine.services.data_readers.stf_xml_reader import STFXMLReader


def test_read_parses_nested_doc_content_nodes():
    xml = b"""<?xml version='1.0' encoding='UTF-8'?>
<ectd:study xmlns:ectd='http://www.ich.org/ectd' xmlns:xlink='http://www.w3.org/1999/xlink' xml:lang='en' dtd-version='2.2'>
  <study-identifier>
    <title>Example Study</title>
    <study-id>ABC-301</study-id>
    <category name='species' info-type='ich'>human</category>
  </study-identifier>
  <study-document>
    <content-block>
      <doc-content xlink:href='../../../index.xml#id1'>
        <title>Top Level Document</title>
        <property name='domain' info-type='us'>DM</property>
        <file-tag name='study-data-reviewers-guide' info-type='ich'/>
      </doc-content>
      <content-block>
        <doc-content xlink:href='../../../index.xml#id2'>
          <title>Nested Document</title>
          <property name='domain' info-type='us'>AE</property>
          <property name='dataset-class' info-type='us'>Events</property>
          <file-tag name='data-tabulation-dataset' info-type='us'/>
        </doc-content>
      </content-block>
    </content-block>
  </study-document>
</ectd:study>
"""

    reader = STFXMLReader()
    metadata = reader.read(xml)

    assert metadata["study_id"] == "ABC-301"
    assert metadata["study_title"] == "Example Study"
    assert metadata["dtd_version"] == "2.2"
    assert metadata["language"] == "en"

    assert metadata["categories"] == [{"name": "species", "info_type": "ich", "value": "human"}]

    assert len(metadata["documents"]) == 2
    assert metadata["documents"][0]["href"] == "../../../index.xml#id1"
    assert metadata["documents"][0]["title"] == "Top Level Document"
    assert metadata["documents"][0]["properties"] == [{"name": "domain", "info_type": "us", "value": "DM"}]
    assert metadata["documents"][0]["file_tags"] == [{"name": "study-data-reviewers-guide", "info_type": "ich"}]

    assert metadata["documents"][1]["href"] == "../../../index.xml#id2"
    assert metadata["documents"][1]["title"] == "Nested Document"
    assert metadata["documents"][1]["properties"] == [
        {"name": "domain", "info_type": "us", "value": "AE"},
        {"name": "dataset-class", "info_type": "us", "value": "Events"},
    ]
    assert metadata["documents"][1]["file_tags"] == [{"name": "data-tabulation-dataset", "info_type": "us"}]
