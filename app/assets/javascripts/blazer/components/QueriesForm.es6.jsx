class QueriesForm extends React.Component {
  constructor(props) {
    super(props)
    this.state = {editorHeight: "160px"}
  }

  componentDidMount() {
    var editor = ace.edit(this._input);
    editor.setTheme("ace/theme/twilight");
    editor.getSession().setMode("ace/mode/sql");
    editor.setOptions({
      enableBasicAutocompletion: false,
      enableSnippets: false,
      enableLiveAutocompletion: false,
      highlightActiveLine: false,
      fontSize: 12,
      minLines: 10
    });
    editor.renderer.setShowGutter(true);
    editor.renderer.setPrintMarginColumn(false);
    editor.renderer.setPadding(10);
    editor.getSession().setUseWrapMode(true);
    editor.commands.addCommand({
      name: 'run',
      bindKey: {win: 'Ctrl-Enter',  mac: 'Command-Enter'},
      exec: function(editor) {
        $("#run").click();
      },
      readOnly: false // false if this command should not apply in readOnly mode
    });

    // http://stackoverflow.com/questions/11584061/
    const adjustHeight = () => {
      let lines = editor.getSession().getScreenLength();
      if (lines < 9) {
        lines = 9;
      }

      const newHeight = (lines + 1) * 16;
      this.setState({editorHeight: newHeight.toString() + "px"})
      editor.resize();
    };

   //  function getSQL() {
   //    var selectedText = editor.getSelectedText();
   //    var text = selectedText.length < 10 ? editor.getValue() : selectedText;
   //    return text.replace(/\n/g, "\r\n");
   //  }

   //  function getErrorLine() {
   //    var error_line = /LINE (\d+)/g.exec($("#results").find('.alert-danger').text());

   //    if (error_line) {
   //      error_line = parseInt(error_line[1], 10);
   //      if (editor.getSelectedText().length >= 10) {
   //        error_line += editor.getSelectionRange().start.row;
   //      }
   //      return error_line;
   //    }
   //  }

    editor.getSession().on("change", adjustHeight);
    adjustHeight();
   //  $("#editor").show();
   //  editor.focus();
  }

  render() {
    return (
      <div>
        <form onSubmit={this.handleSubmit}>
          <div className="row">
            <div className="col-xs-8">
              <div className="form-group">
                <input type="hidden" name="statement" />
                <div id="editor-container">
                  <div id="editor" style={{height: this.state.editorHeight}} ref={(c) => this._input = c}></div>
                </div>
              </div>
              <div className="form-group text-right">
                <div className="pull-left" style={{marginTop: "6px"}}>
                  <a href="">Back</a>
                </div>
                <button onClick={this.runQuery} className="btn btn-info" style={{verticalAlign: "top"}}>Run</button>
              </div>
            </div>
            <div className="col-xs-4">
              <div className="form-group">
                <label htmlFor="name">Name</label>
                <input id="name" type="text" className="form-control" />
              </div>
              <div className="form-group">
                <label htmlFor="description">Description</label>
                <textarea id="description" placeholder="Optional" style={{height: "80px"}} className="form-control"></textarea>
              </div>
              <div className="text-right">
                <input type="submit" className="btn btn-success" value="Create" />
              </div>
            </div>
          </div>
        </form>
        <div id="results"></div>
      </div>
    )
  }

  runQuery(e) {
    e.preventDefault()
    console.log("run")
  }

  handleSubmit(e) {
    e.preventDefault()
    console.log("submit")
    // var data = $(e.target).serialize();
    // console.log(data);
    // $.post("/queries", data, function (data) {
    //   console.log(data);
    //   browserHistory.push('/queries/' + data.id);
    // }.bind(this));
  }
}
