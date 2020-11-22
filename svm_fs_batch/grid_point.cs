namespace svm_fs_batch
{
    internal class grid_point
    {
        internal double? cost;
        internal double? gamma;
        internal double? epsilon;
        internal double? coef0;
        internal double? degree;

        internal double? cv_rate;

        internal grid_point()
        {
            
        }

        internal grid_point(grid_point grid_point)
        {
            this.cost = grid_point.cost;
            this.gamma = grid_point.gamma;
            this.epsilon = grid_point.epsilon;
            this.coef0 = grid_point.coef0;
            this.degree = grid_point.degree;
            this.cv_rate = grid_point.cv_rate;
        }
    }
}
