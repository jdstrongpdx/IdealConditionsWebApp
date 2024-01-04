import IdealConditionsV2 from "../components/IdealConditions/IdealConditionsV2";
import { Link } from "react-router-dom";

function IdealConditionsPage() {
    return (
        <>
<section>

        <article id="about">
          <h2 className="centered"><img src="../images/sun-behind-rain-cloud.png" className="icon" alt="Website Logo"></img> Ideal Conditions</h2>


            <p>Welcome to Ideal Conditions, the best resource for planning your next outdoor activity! Simply enter the
              location you want to be outdoors and this page will display a list of if/when in the next six days the
              weather conditions are ideal for running, fishing, hiking, cycling, camping, hunting, skiing, kayaking, and
              mountaineering.
            </p>
            

        <div class="search_results">
          <p>Example output from this website:</p>

          <p>Running has the following ideal time windows:<br></br>
          <span class="tab"></span>Time window: 2 hour(s) starting at 1/4/2024, 2:00:00 PM<br></br>
          <span class="tab"></span>Weather: mostly Rainy<br></br>
          <span class="tab"></span>Temperature average: 50F <br></br>
          <span class="tab"></span>Wind Speed average: 8 mph<br></br>
          <span class="tab"></span>Humidity average: 94%</p>
        </div>
        </article>

        <article id="usage">
          <h3>How to use this website.</h3>
          <p>Using this website is really easy, just follow the following steps:</p>
          <ol>
            <li>Enter the city, state of the location you want to do an outdoor activity in.</li>
            <li>Click the search button.</li>
            <li>Optional: Enter start and end dates to limit your search to specific days.</li>
            <li>Optional: Check the weekends only box to only show weekend days.</li>
            <li>Scroll through the search results to see the different outdoor activities available and their
            ideal condition time windows.</li>
          </ol>
        </article>

        <article id="form" >
          <h3>Search</h3>
          <IdealConditionsV2/>
        </article>

        <h3>About this Project</h3>
            <article>
                <p>For more information about the development and design of this web app, please see my <Link to="/PortfolioPage">Portfolio</Link> page.</p>
            </article>

        <p>For support or questions about this website, contact <a href="mailto:jdstrongpdx@gmail.com">jdstrongpdx@gmail.com</a></p>


        <br></br>
        <article>
          <p><small><strong>Legal Disclaimer: </strong>This website makes no guarantees as to the accuracy of the information provided. Participation in outdoor activities carries inherent risks and the user accepts all responsibility to verify the weather conditions, safety, and abilities to participate in any activity listed or not.
          </small></p>
        </article>
      </section>



        </>
    );
}

export default IdealConditionsPage;