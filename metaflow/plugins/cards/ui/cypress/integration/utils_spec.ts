import * as utils from "../../src/utils";
import { components } from "../../public/card-example.json";
import type * as types from "../../src/types";

const EXAMPLE_FLOW = "JSONParameterFlow/5536/start/128089";

describe("utils unit tests", () => {
  it("should getPageHierarchy", () => {
    expect(utils.getPageHierarchy(components as types.CardComponent[])).to.eql({
      Run: ["Vertical Table", "Artifacts", "Images", "Horizontal Table"],
      Task: ["DAG", "Log Component", "Line Chart", "Bar Chart"],
    });
  });

  it("should convertPixelsToRem", () => {
    expect(utils.convertPixelsToRem(16)).to.eq(1);
    expect(utils.convertPixelsToRem(10)).to.eq(0.625);
  });

  it("should getPathSpecObject", () => {
    expect(utils.getPathSpecObject(EXAMPLE_FLOW)).to.eql({
      flowname: "JSONParameterFlow",
      runid: "5536",
      stepname: "start",
      taskid: "128089",
    });

    expect(utils.getPathSpecObject("JSONParameterFlow/5536")).to.eql({
      flowname: "JSONParameterFlow",
      runid: "5536",
      stepname: undefined,
      taskid: undefined,
    });
  });

  it("should getFromPathSpec", () => {
    expect(utils.getFromPathSpec(EXAMPLE_FLOW, "flowname")).to.eql(
      "JSONParameterFlow"
    );
    expect(utils.getFromPathSpec(EXAMPLE_FLOW, "runid")).to.equal("5536");
    expect(utils.getFromPathSpec(EXAMPLE_FLOW, "stepname")).to.equal("start");
    expect(utils.getFromPathSpec(EXAMPLE_FLOW, "taskid")).to.equal("128089");
  });

  // SIDE EFFECTS, can't really mock these in unit tests, so lets just make sure they exist
  it("should have side-effect functions", () => {
    expect(utils.isOverflown).to.exist;
    expect(utils.scrollToSection).to.exist;
  });
});

export {};
