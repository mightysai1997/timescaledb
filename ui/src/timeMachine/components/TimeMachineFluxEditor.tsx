// Libraries
import React, {FC, useState} from 'react'
import {connect} from 'react-redux'

// Components
import FluxEditor from 'src/shared/components/FluxMonacoEditor'
import FluxToolbar from 'src/timeMachine/components/FluxToolbar'

// Actions
import {setActiveQueryText} from 'src/timeMachine/actions'
import {saveAndExecuteQueries} from 'src/timeMachine/actions/queries'

// Utils
import {getActiveQuery, getActiveTimeMachine} from 'src/timeMachine/selectors'
import {
  formatFunctionForInsert,
  generateImport,
} from 'src/timeMachine/utils/insertFunction'

// Types
import {AppState, FluxToolbarFunction, EditorType} from 'src/types'

interface StateProps {
  activeQueryText: string
  activeTab: string
}

interface DispatchProps {
  onSetActiveQueryText: typeof setActiveQueryText
  onSubmitQueries: typeof saveAndExecuteQueries
}

type Props = StateProps & DispatchProps

const TimeMachineFluxEditor: FC<Props> = ({
  activeQueryText,
  onSubmitQueries,
  onSetActiveQueryText,
  activeTab,
}) => {
  const [editorInstance, setEditorInstance] = useState<EditorType>(null)

  const handleInsertVariable = (variableName: string): void => {
    const p = editorInstance.getPosition()
    editorInstance.executeEdits('', [
      {
        range: new window.monaco.Range(
          p.lineNumber,
          p.column,
          p.lineNumber,
          p.column
        ),
        text: `v.${variableName}`,
      },
    ])
    onSetActiveQueryText(editorInstance.getValue())
  }

  const handleInsertFluxFunction = (func: FluxToolbarFunction): void => {
    const p = editorInstance.getPosition()
    // sets the range based on the current position
    let range = new window.monaco.Range(
      p.lineNumber,
      p.column,
      p.lineNumber,
      p.column
    )
    // edge case for when user toggles to the script editor
    // this defaults the cursor to the initial position (top-left, 1:1 position)
    if (p.lineNumber === 1 && p.column === 1) {
      const [currentRange] = editorInstance.getVisibleRanges()
      // adds the function to the end of the query
      range = new window.monaco.Range(
        currentRange.endLineNumber + 1,
        p.column,
        currentRange.endLineNumber + 1,
        p.column
      )
    }
    const edits = [
      {
        range,
        text: formatFunctionForInsert(func.name, func.example),
      },
    ]
    const importStatement = generateImport(
      func.package,
      editorInstance.getValue()
    )
    if (importStatement) {
      edits.unshift({
        range: new window.monaco.Range(1, 1, 1, 1),
        text: `${importStatement}\n`,
      })
    }
    editorInstance.executeEdits('', edits)
    onSetActiveQueryText(editorInstance.getValue())
  }

  return (
    <div className="flux-editor">
      <div className="flux-editor--left-panel">
        <FluxEditor
          script={activeQueryText}
          onChangeScript={onSetActiveQueryText}
          onSubmitScript={onSubmitQueries}
          setEditorInstance={setEditorInstance}
        />
      </div>
      <div className="flux-editor--right-panel">
        <FluxToolbar
          activeQueryBuilderTab={activeTab}
          onInsertFluxFunction={handleInsertFluxFunction}
          onInsertVariable={handleInsertVariable}
        />
      </div>
    </div>
  )
}

const mstp = (state: AppState) => {
  const activeQueryText = getActiveQuery(state).text
  const {activeTab} = getActiveTimeMachine(state)

  return {activeQueryText, activeTab}
}

const mdtp = {
  onSetActiveQueryText: setActiveQueryText,
  onSubmitQueries: saveAndExecuteQueries,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(TimeMachineFluxEditor)
