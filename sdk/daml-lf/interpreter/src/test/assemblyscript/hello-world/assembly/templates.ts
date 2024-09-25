import * as lf from "../protobuf/com/digitalasset/daml/lf/value/Value"
import * as api from "./ledger/api"

@template(precond=true, signatories=["@arg.owner"], observers=[])
class SimpleTemplate {
	@arg
	private owner: lf.Identifier
	@arg
	private count: i64

	@choice(consuming=true, controllers=[this.owner], observers=[], authorizers=[])
	SimpleTemplate_increment(n: i64): ContractId<SimpleTemplate> {
		api.logInfo(`called AssemblyScript SimpleTemplate_increment(${n}) with count = ${this.count}`)

		return new SimpleTemplate(this.owner, this.count + n)
	}
}
