// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Dropdown,
  DropdownMenuTheme,
  ComponentStatus,
} from '@influxdata/clockface'

// Actions
import {selectValue} from 'src/variables/actions/creators'

// Utils
import {getVariable} from 'src/variables/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
  values: string[]
  selectedValue: string
}

interface DispatchProps {
  onSelectValue: typeof selectValue
}

interface OwnProps {
  variableID: string
  contextID: string
  testID?: string
  onSelect?: () => void
}

type Props = StateProps & DispatchProps & OwnProps

class VariableDropdown extends PureComponent<Props> {
  render() {
    const {selectedValue, values} = this.props

    const dropdownStatus =
      values.length === 0 ? ComponentStatus.Disabled : ComponentStatus.Default

    return (
      <div className="variable-dropdown">
        {/* TODO: Add variable description to title attribute when it is ready */}
        <Dropdown
          style={{width: `${140}px`}}
          className="variable-dropdown--dropdown"
          testID={this.props.testID || 'variable-dropdown'}
          button={(active, onClick) => (
            <Dropdown.Button
              active={active}
              onClick={onClick}
              testID="variable-dropdown--button"
              status={dropdownStatus}
            >
              {selectedValue || 'No Values'}
            </Dropdown.Button>
          )}
          menu={onCollapse => (
            <Dropdown.Menu
              onCollapse={onCollapse}
              theme={DropdownMenuTheme.Amethyst}
            >
              {values.map(val => {
                return (
                  <Dropdown.Item
                    key={val}
                    id={val}
                    value={val}
                    onClick={this.handleSelect}
                    selected={val === selectedValue}
                    testID="variable-dropdown--item"
                  >
                    {val}
                  </Dropdown.Item>
                )
              })}
            </Dropdown.Menu>
          )}
        />
      </div>
    )
  }

  private handleSelect = (selectedValue: string) => {
    const {contextID, variableID, onSelectValue, onSelect} = this.props

    onSelectValue(contextID, variableID, selectedValue)

    if (onSelect) {
      onSelect()
    }
  }
}

const mstp = (state: AppState, props: OwnProps): StateProps => {
  const {contextID, variableID} = props

  const variable = getVariable(state, contextID, variableID)
  const type = variable.arguments.type

  if (type === 'constant') {
    return {
      values: variable.arguments.values,
      selectedValue: variable.selected[0],
    }
  }

  if (type === 'map') {
    return {
      values: Object.keys(variable.arguments.values),
      selectedValue: variable.selected[0],
    }
  }

  if (type === 'query') {
    return {
      values: variable.arguments.values.results || [],
      selectedValue: variable.selected[0],
    }
  }

  return {
    values: [],
    selectedValue: '',
  }
}

const mdtp = {
  onSelectValue: selectValue,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VariableDropdown)
